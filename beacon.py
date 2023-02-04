# Beacon eddy current scanner support
#
# Copyright (C) 2020-2023 Matt Baker <baker.matt.j@gmail.com>
# Copyright (C) 2020-2023 Lasse Dalegaard <dalegaard@gmail.com>
# Copyright (C) 2023 Beacon <beacon3d.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license.
import threading
import logging
import chelper
import pins
import math
import time
import queue
import json
import struct
import numpy as np
import copy
from numpy.polynomial import Polynomial
from . import manual_probe
from . import probe
from . import bed_mesh
from . import thermistor
from . import adc_temperature
from mcu import MCU, MCU_trsync
from clocksync import SecondarySync

STREAM_BUFFER_LIMIT_DEFAULT = 100

class BeaconProbe:
    def __init__(self, config):
        self.printer = config.get_printer()
        self.reactor = self.printer.get_reactor()
        self.name = config.get_name()

        self.speed = config.getfloat('speed', 5.0, above=0.)
        self.lift_speed = config.getfloat('lift_speed', self.speed, above=0.)
        self.backlash_comp = config.getfloat('backlash_comp', 0.5)

        self.x_offset = config.getfloat('x_offset', 0.)
        self.y_offset = config.getfloat('y_offset', 0.)

        self.trigger_distance = config.getfloat('trigger_distance', 2.)
        self.trigger_dive_threshold = config.getfloat(
                'trigger_dive_threshold', 1.)
        self.trigger_hysteresis = config.getfloat('trigger_hysteresis', 0.006)

        # If using paper for calibration, this would be .1mm
        self.cal_nozzle_z = config.getfloat('cal_nozzle_z', 0.1)
        self.cal_floor = config.getfloat('cal_floor', 0.2)
        self.cal_ceil = config.getfloat('cal_ceil', 5.)
        self.cal_speed = config.getfloat('cal_speed', 1.)
        self.cal_move_speed = config.getfloat('cal_move_speed', 10.)

        # Load models
        self.model = None
        self.models = {}
        self.model_temp_builder = BeaconTempModelBuilder.load(config)
        self.model_temp = None
        self.default_model_name = config.get('default_model_name', 'default')
        self.model_manager = ModelManager(self)

        # Temperature sensor integration
        self.last_temp = 0
        self.measured_min = 99999999.
        self.measured_max = 0.

        self.mesh_helper = BeaconMeshHelper.create(self, config)

        self._stream_en = 0
        self._stream_callbacks = {}
        self._stream_latency_requests = {}
        self._stream_buffer = []
        self._stream_buffer_limit = STREAM_BUFFER_LIMIT_DEFAULT
        self._stream_buffer_limit_new = self._stream_buffer_limit
        self._stream_samples_queue = queue.Queue()
        self._stream_flush_event = threading.Event()
        self._log_stream = None
        self._data_filter = AlphaBetaFilter(
            config.getfloat('filter_alpha', 0.5),
            config.getfloat('filter_beta', 0.000001),
        )
        self.trapq = None

        mainsync = self.printer.lookup_object('mcu')._clocksync
        self._mcu = MCU(config, SecondarySync(self.reactor, mainsync))
        self.printer.add_object('mcu ' + self.name, self._mcu)
        self.cmd_queue = self._mcu.alloc_command_queue()
        self.mcu_probe = BeaconEndstopWrapper(self)

        # Register z_virtual_endstop
        self.printer.lookup_object('pins').register_chip('probe', self)
        # Register event handlers
        self.printer.register_event_handler('klippy:connect',
                                            self._handle_connect)
        self.printer.register_event_handler('klippy:mcu_identify',
                                            self._handle_mcu_identify)
        self._mcu.register_config_callback(self._build_config)
        self._mcu.register_response(self._handle_beacon_data, "beacon_data")
        # Register webhooks
        webhooks = self.printer.lookup_object('webhooks')
        self._api_dump_helper = APIDumpHelper(self)
        webhooks.register_endpoint('beacon/status', self._handle_req_status)
        webhooks.register_endpoint('beacon/dump', self._handle_req_dump)
        # Register gcode commands
        self.gcode = self.printer.lookup_object('gcode')
        self.gcode.register_command('BEACON_STREAM', self.cmd_BEACON_STREAM,
                                    desc=self.cmd_BEACON_STREAM_help)
        self.gcode.register_command('BEACON_QUERY', self.cmd_BEACON_QUERY,
                                    desc=self.cmd_BEACON_QUERY_help)
        self.gcode.register_command('BEACON_CALIBRATE',
                                    self.cmd_BEACON_CALIBRATE,
                                    desc=self.cmd_BEACON_CALIBRATE_help)
        self.gcode.register_command('BEACON_ESTIMATE_BACKLASH',
                                    self.cmd_BEACON_ESTIMATE_BACKLASH,
                                    desc=self.cmd_BEACON_ESTIMATE_BACKLASH_help)
        self.gcode.register_command('PROBE', self.cmd_PROBE,
                                    desc=self.cmd_PROBE_help)
        self.gcode.register_command('PROBE_ACCURACY', self.cmd_PROBE_ACCURACY,
                                    desc=self.cmd_PROBE_ACCURACY_help)
        self.gcode.register_command('Z_OFFSET_APPLY_PROBE',
                                    self.cmd_Z_OFFSET_APPLY_PROBE,
                                    desc=self.cmd_Z_OFFSET_APPLY_PROBE_help)

    # Event handlers

    def _handle_connect(self):
        self.phoming = self.printer.lookup_object('homing')

        # Ensure streaming mode is stopped
        self.beacon_stream_cmd.send([0])

        self.model_temp = self.model_temp_builder.build_with_nvm(self)
        self.model = self.models.get(self.default_model_name, None)
        if self.model:
            self._apply_threshold()

    def _handle_mcu_identify(self):
        constants = self._mcu.get_constants()

        self._mcu_freq = self._mcu._mcu_freq

        self.inv_adc_max = 1.0 / constants.get("ADC_MAX")
        self.temp_smooth_count = constants.get('BEACON_ADC_SMOOTH_COUNT')
        self.thermistor = thermistor.Thermistor(10000., 0.)
        self.thermistor.setup_coefficients_beta(25., 47000., 4101.)

        self.toolhead = self.printer.lookup_object("toolhead")
        self.trapq = self.toolhead.get_trapq()

    def _build_config(self):
        self.beacon_stream_cmd = self._mcu.lookup_command(
            "beacon_stream en=%u", cq=self.cmd_queue)
        self.beacon_set_threshold = self._mcu.lookup_command(
            "beacon_set_threshold trigger=%u untrigger=%u", cq=self.cmd_queue)
        self.beacon_home_cmd = self._mcu.lookup_command(
            "beacon_home trsync_oid=%c trigger_reason=%c trigger_invert=%c",
            cq=self.cmd_queue)
        self.beacon_stop_home = self._mcu.lookup_command(
            "beacon_stop_home", cq=self.cmd_queue)
        self.beacon_nvm_read_cmd = self._mcu.lookup_query_command(
            "beacon_nvm_read len=%c offset=%hu",
            "beacon_nvm_data bytes=%*s offset=%hu",
            cq=self.cmd_queue)

    def stats(self, eventtime):
        return False, '%s: coil_temp=%.1f' % (self.name, self.last_temp)

    # Virtual endstop

    def setup_pin(self, pin_type, pin_params):
        if pin_type != 'endstop' or pin_params['pin'] != 'z_virtual_endstop':
            raise pins.error("Probe virtual endstop only useful as endstop pin")
        if pin_params['invert'] or pin_params['pullup']:
            raise pins.error("Can not pullup/invert probe virtual endstop")
        return self.mcu_probe

    # Probe interface

    def multi_probe_begin(self):
        self._start_streaming()

    def multi_probe_end(self):
        self._stop_streaming()

    def get_offsets(self):
        return self.x_offset, self.y_offset, self.trigger_distance

    def get_lift_speed(self, gcmd=None):
        if gcmd is not None:
            return gcmd.get_float("LIFT_SPEED", self.lift_speed, above=0.)
        return self.lift_speed

    def run_probe(self, gcmd):
        if self.model is None:
            raise self.printer.command_error("No Beacon model loaded")

        speed = gcmd.get_float("PROBE_SPEED", self.speed, above=0.)
        lift_speed = self.get_lift_speed(gcmd)
        toolhead = self.printer.lookup_object('toolhead')
        curtime = self.reactor.monotonic()
        if 'z' not in toolhead.get_status(curtime)['homed_axes']:
            raise self.printer.command_error("Must home before probe")

        self._start_streaming()
        try:
            return self._probe(speed)
        finally:
            self._stop_streaming()

    def _move_to_probing_height(self, speed):
        target = self.trigger_distance
        top = target + self.backlash_comp
        cur_z = self.toolhead.get_position()[2]
        if cur_z < top:
            self.toolhead.manual_move([None, None, top], speed)
        self.toolhead.manual_move([None, None, target], speed)
        self.toolhead.wait_moves()

    def _probing_move_to_probing_height(self, speed):
        curtime = self.reactor.monotonic()
        status = self.toolhead.get_kinematics().get_status(curtime)
        pos = self.toolhead.get_position()
        pos[2] = status['axis_minimum'][2]
        try:
            self.phoming.probing_move(self.mcu_probe, pos, speed)
            samples = self._sample_printtime_sync(50)
        except self.printer.command_error as e:
            reason = str(e)
            if "Timeout during probing move" in reason:
                reason += probe.HINT_TIMEOUT
            raise self.printer.command_error(reason)

    def _sample(self, num_samples):
        samples = self._sample_printtime_sync(5, num_samples)
        return (median([s['dist'] for s in samples]), samples)

    def _probe(self, speed, num_samples=10):
        target = self.trigger_distance
        tdt = self.trigger_dive_threshold
        (dist, samples) = self._sample(num_samples)

        if dist > target + tdt:
            # If we are above the dive threshold right now, we'll need to
            # do probing move and then re-measure
            self._probing_move_to_probing_height(speed)
            (dist, samples) = self._sample(num_samples)
        elif self.toolhead.get_position()[2] < target - tdt:
            # We are below the probing target height, we'll move to the
            # correct height and take a new sample.
            self._move_to_probing_height(speed)
            (dist, samples) = self._sample(num_samples)

        pos = samples[0]['pos']

        self.gcode.respond_info("probe at %.3f,%.3f,%.3f is z=%.6f"
                                % (pos[0], pos[1], pos[2], dist))

        return [pos[0], pos[1], pos[2] + target - dist]

    # Calibration routines

    def _start_calibration(self, gcmd):
        if gcmd.get("SKIP_MANUAL_PROBE", None) is not None:
            kin = self.toolhead.get_kinematics()
            kin_spos = {s.get_name(): s.get_commanded_position()
                        for s in kin.get_steppers()}
            kin_pos = kin.calc_position(kin_spos)
            self._calibrate(gcmd, kin_pos)
        else:
            curtime = self.printer.get_reactor().monotonic()
            kin_status = self.toolhead.get_status(curtime)
            if 'xy' not in kin_status['homed_axes']:
                raise self.printer.command_error("Must home X and Y "
                                                 "before calibration")

            forced_z = False
            if 'z' not in kin_status['homed_axes']:
                self.toolhead.get_last_move_time()
                pos = self.toolhead.get_position()
                pos[2] = kin_status['axis_maximum'][2]
                self.toolhead.set_position(pos, homing_axes=[2])
                forced_z = True

            cb = lambda kin_pos: self._calibrate(gcmd, kin_pos, forced_z)
            manual_probe.ManualProbeHelper(self.printer, gcmd, cb)
    def _calibrate(self, gcmd, kin_pos, forced_z):
        if kin_pos is None:
            if forced_z:
                kin = self.toolhead.get_kinematics()
                if hasattr(kin, "note_z_not_homed"):
                        kin.note_z_not_homed()
            return

        gcmd.respond_info("Beacon calibration starting")
        cal_nozzle_z = gcmd.get_float('NOZZLE_Z', self.cal_nozzle_z)
        cal_floor = gcmd.get_float('FLOOR', self.cal_floor)
        cal_ceil = gcmd.get_float('CEIL', self.cal_ceil)
        cal_min_z = kin_pos[2] - cal_nozzle_z + cal_floor
        cal_max_z = kin_pos[2] - cal_nozzle_z + cal_ceil
        cal_speed = gcmd.get_float('SPEED', self.cal_speed)
        move_speed = gcmd.get_float('MOVE_SPEED', self.cal_move_speed)

        toolhead = self.toolhead
        curtime = self.reactor.monotonic()
        toolhead.wait_moves()
        pos = toolhead.get_position()

        # Move over to probe coordinate and pull out backlash
        curpos = self.toolhead.get_position()
        curpos[2] = cal_max_z + self.backlash_comp
        toolhead.manual_move(curpos, move_speed) # Up
        curpos[0] -= self.x_offset
        curpos[1] -= self.y_offset
        toolhead.manual_move(curpos, move_speed) # Over
        curpos[2] = cal_max_z
        toolhead.manual_move(curpos, move_speed) # Down
        toolhead.wait_moves()

        samples = []
        def cb(sample):
            samples.append(sample)

        try:
            self._start_streaming()
            self._sample_printtime_sync(50)
            with self.streaming_session(cb) as ss:
                self._sample_printtime_sync(50)
                toolhead.dwell(0.250)
                curpos[2] = cal_min_z
                toolhead.manual_move(curpos, cal_speed)
                toolhead.dwell(0.250)
                self._sample_printtime_sync(50)
        finally:
            self._stop_streaming()

        # Fit the sampled data
        z_offset = [s['pos'][2]-cal_min_z+cal_floor
                    for s in samples]
        freq = [s['freq'] for s in samples]
        temp = [s['temp'] for s in samples]
        inv_freq = [1/f for f in freq]
        poly = Polynomial.fit(inv_freq, z_offset, 9)
        temp_median = median(temp)
        self.model = BeaconModel("default",
                                 self, poly, temp_median,
                                 min(z_offset), max(z_offset))
        self.models[self.model.name] = self.model
        self.model.save(self)
        self._apply_threshold()

        self.toolhead.get_last_move_time()
        pos = self.toolhead.get_position()
        pos[2] = cal_floor
        self.toolhead.set_position(pos)

        # Dump calibration curve
        fn = "/tmp/beacon-calibrate-"+time.strftime("%Y%m%d_%H%M%S")+".csv"
        f = open(fn, "w")
        f.write("freq,z,temp\n")
        for i in range(len(freq)):
            f.write("%.5f,%.5f,%.3f\n" % (freq[i], z_offset[i], temp[i]))
        f.close()

        gcmd.respond_info("Beacon calibrated at %.3f,%.3f from "
                          "%.3f to %.3f, speed %.2f mm/s, temp %.2fC"
                          % (pos[0], pos[1],
                          cal_min_z, cal_max_z, cal_speed, temp_median))

    # Internal

    def _update_thresholds(self, moving_up=False):
        self.trigger_freq = self.dist_to_freq(self.trigger_distance, self.last_temp)
        self.untrigger_freq = self.trigger_freq * (1-self.trigger_hysteresis)

    def _apply_threshold(self, moving_up=False):
        self._update_thresholds()
        trigger_c = int(self.freq_to_count(self.trigger_freq))
        untrigger_c = int(self.freq_to_count(self.untrigger_freq))
        self.beacon_set_threshold.send([trigger_c, untrigger_c])

    def _register_model(self, name, model):
        if name in self.models:
            raise self.printer.config_error("Multiple Beacon models with same"
                                            "name '%s'" % (name,))
        self.models[name] = model

    # Streaming mode

    def _enrich_sample_time(self, sample):
        clock = sample['clock'] = self._mcu.clock32_to_clock64(sample['clock'])
        sample['time'] = self._mcu.clock_to_print_time(clock)

    def _enrich_sample_temp(self, sample):
        temp_adc = sample['temp'] / self.temp_smooth_count * self.inv_adc_max
        sample['temp'] = self.thermistor.calc_temp(temp_adc)

    def _enrich_sample(self, sample):
        sample['data_smooth'] = self._data_filter.value()
        freq = sample['freq'] = self.count_to_freq(sample['data_smooth'])
        sample['dist'] = self.freq_to_dist(freq, sample['temp'])
        pos, vel = self._get_trapq_position(sample['time'])

        if pos is None:
            return
        sample['pos'] = pos
        sample['vel'] = vel

    def _start_streaming(self):
        if self._stream_en == 0:
            self.beacon_stream_cmd.send([1])
        self._stream_en += 1
        self._data_filter.reset()
        self._stream_flush()
    def _stop_streaming(self):
        self._stream_en -= 1
        if self._stream_en == 0:
            self.beacon_stream_cmd.send([0])
        self._stream_flush()

    def request_stream_latency(self, latency):
        next_key = 0
        if self._stream_latency_requests:
            next_key = max(self._stream_latency_requests.keys()) + 1
        new_limit = STREAM_BUFFER_LIMIT_DEFAULT
        self._stream_latency_requests[next_key] = latency
        min_requested = min(self._stream_latency_requests.values())
        if min_requested < new_limit:
            new_limit = min_requested
        if new_limit < 1:
            new_limit = 1
        self._stream_buffer_limit_new = new_limit
        return next_key

    def drop_stream_latency_request(self, key):
        self._stream_latency_requests.pop(key, None)
        new_limit = STREAM_BUFFER_LIMIT_DEFAULT
        if self._stream_latency_requests:
            min_requested = min(self._stream_latency_requests.values())
            if min_requested < new_limit:
                new_limit = min_requested
        if new_limit < 1:
            new_limit = 1
        self._stream_buffer_limit_new = new_limit

    def streaming_session(self, callback, completion_callback=None, latency=None):
        return StreamingHelper(self, callback, completion_callback, latency)

    def _stream_flush(self):
        self._stream_flush_event.clear()
        while True:
            try:
                samples = self._stream_samples_queue.get_nowait()
                for sample in samples:
                    self._enrich_sample_temp(sample)

                    temp = sample['temp']
                    self.last_temp = temp
                    if temp:
                        self.measured_min = min(self.measured_min, temp)
                        self.measured_max = max(self.measured_max, temp)

                    self._enrich_sample_time(sample)
                    self._data_filter.update(sample['time'], sample['data'])

                    if len(self._stream_callbacks) > 0:
                        self._enrich_sample(sample)
                        for cb in self._stream_callbacks.values():
                            cb(sample)
            except queue.Empty:
                return

    def _stream_flush_schedule(self):
        force = self._stream_en == 0 # When streaming is disabled, let all through
        if self._stream_buffer_limit_new != self._stream_buffer_limit:
            force = True
            self._stream_buffer_limit = self._stream_buffer_limit_new
        if not force and len(self._stream_buffer) < self._stream_buffer_limit:
            return
        self._stream_samples_queue.put_nowait(self._stream_buffer)
        self._stream_buffer = []
        if self._stream_flush_event.is_set():
            return
        self._stream_flush_event.set()
        self.reactor.register_async_callback(lambda e: self._stream_flush())

    def _handle_beacon_data(self, params):
        if self.trapq is None:
            return

        self._stream_buffer.append(params.copy())
        self._stream_flush_schedule()

    def _get_trapq_position(self, print_time):
        ffi_main, ffi_lib = chelper.get_ffi()
        data = ffi_main.new('struct pull_move[1]')
        count = ffi_lib.trapq_extract_old(self.trapq, data, 1, 0., print_time)
        if not count:
            return None, None
        move = data[0]
        move_time = max(0., min(move.move_t, print_time - move.print_time))
        dist = (move.start_v + .5 * move.accel * move_time) * move_time
        pos = (move.start_x + move.x_r * dist, move.start_y + move.y_r * dist,
               move.start_z + move.z_r * dist)
        velocity = move.start_v + move.accel * move_time
        return pos, velocity

    def _sample_printtime_sync(self, skip=0, count=1):
        toolhead = self.printer.lookup_object('toolhead')
        move_time = toolhead.get_last_move_time()
        settle_clock = self._mcu.print_time_to_clock(move_time)
        samples = []
        total = skip + count

        def cb(sample):
            if sample['clock'] >= settle_clock:
                samples.append(sample)
                if len(samples) >= total:
                    raise StopStreaming

        with self.streaming_session(cb, latency=skip+count) as ss:
            ss.wait()

        samples = samples[skip:]

        if count == 1:
            return samples[0]
        else:
            return samples

    def _sample_async(self, count=1):
        samples = []
        def cb(sample):
            samples.append(sample)
            if len(samples) >= count:
                raise StopStreaming

        with self.streaming_session(cb, latency=count) as ss:
            ss.wait()

        if count == 1:
            return samples[0]
        else:
            return samples

    def count_to_freq(self, count):
        return count*self._mcu_freq/(2**28)

    def freq_to_count(self, freq):
        return freq*(2**28)/self._mcu_freq

    def dist_to_freq(self, dist, temp):
        if self.model is None:
            return None
        return self.model.dist_to_freq(dist, temp)

    def freq_to_dist(self, freq, temp):
        if self.model is None:
            return None
        return self.model.freq_to_dist(freq, temp)

    # Webhook handlers

    def _handle_req_status(self, web_request):
        temp = None
        sample = self._sample_async()
        out = {
            'freq': sample['freq'],
            'dist': sample['dist'],
        }
        temp = sample['temp']
        if temp is not None:
            out['temp'] = temp
        web_request.send(out)

    def _handle_req_dump(self, web_request):
        self._api_dump_helper.add_client(web_request)

    # GCode command handlers

    cmd_PROBE_help = "Probe Z-height at current XY position"
    def cmd_PROBE(self, gcmd):
        pos = self.run_probe(gcmd)
        gcmd.respond_info("Result is z=%.6f" % (pos[2],))

    cmd_BEACON_CALIBRATE_help = "Calibrate beacon response curve"
    def cmd_BEACON_CALIBRATE(self,gcmd):
        self._start_calibration(gcmd)

    cmd_BEACON_ESTIMATE_BACKLASH_help = "Estimate Z axis backlash"
    def cmd_BEACON_ESTIMATE_BACKLASH(self, gcmd):
        # Get to correct Z height
        overrun = gcmd.get_float('OVERRUN', 1.)
        speed = gcmd.get_float("PROBE_SPEED", self.speed, above=0.)
        cur_z = self.toolhead.get_position()[2]
        self.toolhead.manual_move([None, None, cur_z+overrun], speed)
        self.run_probe(gcmd)

        lift_speed = self.get_lift_speed(gcmd)
        target = gcmd.get_float('Z', self.trigger_distance)

        num_samples = gcmd.get_int('SAMPLES', 20)

        samples_up = []
        samples_down = []

        next_dir = -1

        try:
            self._start_streaming()

            (cur_dist, _samples) = self._sample(10)
            pos = self.toolhead.get_position()
            missing = target - cur_dist
            target = pos[2] + missing
            gcmd.respond_info("Target kinematic Z is %.3f" % (target,))

            if target - overrun < 0:
                raise gcmd.error("Target minus overrun must exceed 0mm")

            while len(samples_up) + len(samples_down) < num_samples:
                liftpos = [None, None, target + overrun * next_dir]
                self.toolhead.manual_move(liftpos, lift_speed)
                liftpos = [None, None, target]
                self.toolhead.manual_move(liftpos, lift_speed)
                self.toolhead.wait_moves()
                (dist, _samples) = self._sample(10)
                {-1: samples_up, 1: samples_down}[next_dir].append(dist)
                next_dir = next_dir * -1

        finally:
            self._stop_streaming()

        res_up = median(samples_up)
        res_down = median(samples_down)

        gcmd.respond_info("Median distance moving up %.5f, down %.5f, "
                          "delta %.5f over %d samples" %
                          (res_up, res_down, res_down - res_up,
                           num_samples))

    cmd_BEACON_QUERY_help = "Take a sample from the sensor"
    def cmd_BEACON_QUERY(self, gcmd):
        sample = self._sample_async()
        last_value = sample['freq']
        dist = sample['dist']
        temp = sample['temp']
        if dist is None:
            gcmd.respond_info("Last reading: %.2fHz, %.2fC, no model" %
                              (last_value, temp,))
        else:
            gcmd.respond_info("Last reading: %.2fHz, %.2fC, %.5fmm" %
                              (last_value, temp, dist))

    cmd_BEACON_STREAM_help = "Enable Beacon Streaming"
    def cmd_BEACON_STREAM(self, gcmd):
        if self._log_stream is not None:
            self._log_stream.stop()
            self._log_stream = None
            gcmd.respond_info("Beacon Streaming disabled")
        else:
            f = None
            completion_cb = None
            fn = gcmd.get("FILENAME")
            f = open(fn, "w")
            def close_file():
                f.close()
            completion_cb = close_file
            f.write("time,data,data_smooth,freq,dist,temp,pos_x,pos_y,pos_z,vel\n")

            def cb(sample):
                pos = sample.get('pos', None)
                obj = "%.4f,%d,%.2f,%.5f,%.5f,%.2f,%s,%s,%s,%s\n" % (
                    sample['time'],
                    sample['data'],
                    sample['data_smooth'],
                    sample['freq'],
                    sample['dist'],
                    sample['temp'],
                    "%.3f" % (pos[0],) if pos is not None else "",
                    "%.3f" % (pos[1],) if pos is not None else "",
                    "%.3f" % (pos[2],) if pos is not None else "",
                    "%.3f" % (sample['vel'],) if 'vel' in sample else ""
                )
                f.write(obj)

            self._log_stream = self.streaming_session(cb, completion_cb)
            gcmd.respond_info("Beacon Streaming enabled")

    cmd_PROBE_ACCURACY_help = "Probe Z-height accuracy at current XY position"
    def cmd_PROBE_ACCURACY(self, gcmd):
        speed = gcmd.get_float("PROBE_SPEED", self.speed, above=0.)
        lift_speed = self.get_lift_speed(gcmd)
        sample_count = gcmd.get_int("SAMPLES", 10, minval=1)
        sample_retract_dist = gcmd.get_float("SAMPLE_RETRACT_DIST", 0)
        pos = self.toolhead.get_position()
        gcmd.respond_info("PROBE_ACCURACY at X:%.3f Y:%.3f Z:%.3f"
                          " (samples=%d retract=%.3f"
                          " speed=%.1f lift_speed=%.1f)\n"
                          % (pos[0], pos[1], pos[2],
                             sample_count, sample_retract_dist,
                             speed, lift_speed))

        start_height = self.trigger_distance + sample_retract_dist
        liftpos = [None, None, start_height]
        self.toolhead.manual_move(liftpos, lift_speed)

        self.multi_probe_begin()
        positions = []
        while len(positions) < sample_count:
            pos = self._probe(speed)
            positions.append(pos)
            self.toolhead.manual_move(liftpos, lift_speed)
        self.multi_probe_end()

        zs = [p[2] for p in positions]
        max_value = max(zs)
        min_value = min(zs)
        range_value = max_value - min_value
        avg_value = sum(zs) / len(positions)
        median_ = median(zs)

        deviation_sum = 0
        for i in range(len(zs)):
            deviation_sum += pow(zs[2] - avg_value, 2.)
        sigma = (deviation_sum / len(zs)) ** 0.5

        gcmd.respond_info(
            "probe accuracy results: maximum %.6f, minimum %.6f, range %.6f, "
            "average %.6f, median %.6f, standard deviation %.6f" % (
            max_value, min_value, range_value, avg_value, median_, sigma))

    cmd_Z_OFFSET_APPLY_PROBE_help = "Adjust the probe's z_offset"
    def cmd_Z_OFFSET_APPLY_PROBE(self, gcmd):
        gcode_move = self.printer.lookup_object("gcode_move")
        offset = gcode_move.get_status()['homing_origin'].z

        if offset == 0:
            self.gcode.respond_info("Nothing to do: Z Offset is 0")
            return

        if not self.model:
            raise self.gcode.error("You must calibrate your model first, "
                                   "use BEACON_CALIBRATE.")

        # We use the model code to save the new offset, but we can't actually
        # apply that offset yet because the gcode_offset is still in effect.
        # If the user continues to do stuff after this, the newly set model
        # offset would compound with the gcode offset. To ensure this doesn't
        # happen, we revert to the old model offset afterwards.
        # Really, the user should just be calling `SAVE_CONFIG` now.
        old_offset = self.model.offset
        self.model.offset += offset
        self.model.save(self, False)
        gcmd.respond_info("Beacon model offset has been updated\n"
                "You must run the SAVE_CONFIG command now to update the\n"
                "printer config file and restart the printer.")
        self.model.offset = old_offset

class BeaconModel:
    @classmethod
    def load(cls, name, config, beacon):
        coef = config.getfloatlist('model_coef')
        temp = config.getfloat('model_temp')
        domain = config.getfloatlist('model_domain', count=2)
        [min_z, max_z] = config.getfloatlist('model_range', count=2)
        offset = config.getfloat('model_offset', 0.)
        poly = Polynomial(coef, domain)
        return BeaconModel(name, beacon, poly, temp, min_z, max_z, offset)

    def __init__(self, name, beacon, poly, temp, min_z, max_z, offset=0):
        self.name = name
        self.beacon = beacon
        self.poly = poly
        self.min_z = min_z
        self.max_z = max_z
        self.temp = temp
        self.offset = offset

    def save(self, beacon, show_message=True):
        configfile = beacon.printer.lookup_object('configfile')
        section = "beacon model " + self.name
        configfile.set(section, 'model_coef',
                       ",\n  ".join(map(str, self.poly.coef)))
        configfile.set(section, 'model_domain',
                       ",".join(map(str, self.poly.domain)))
        configfile.set(section, 'model_range',
                       "%f,%f" % (self.min_z, self.max_z))
        configfile.set(section, 'model_temp',
                       "%f" % (self.temp))
        configfile.set(section, 'model_offset', "%.5f" % (self.offset,))
        if show_message:
            beacon.gcode.respond_info("Beacon calibration for model '%s' has "
                    "been updated\nfor the current session. The SAVE_CONFIG "
                    "command will\nupdate the printer config file and restart "
                    "the printer." % (self.name,))

    def freq_to_dist_raw(self, freq):
        return float(self.poly(1/freq) - self.offset)

    def freq_to_dist(self, freq, temp):
        if self.temp is not None and \
            self.beacon.model_temp is not None:
            freq = self.beacon.model_temp.compensate(
                            freq, temp, self.temp)
        return self.freq_to_dist_raw(freq)

    def dist_to_freq_raw(self, dist, max_e=0.00000001):
        dist += self.offset
        [begin, end] = self.poly.domain
        for _ in range(0, 50):
            f = (end + begin) / 2
            v = self.poly(f)
            if abs(v-dist) < max_e:
                return float(1./f)
            elif v < dist:
                begin = f
            else:
                end = f
        raise beacon.printer.command_error("Beacon model convergence error")

    def dist_to_freq(self, dist, temp, max_e=0.00000001):
        freq = self.dist_to_freq_raw(dist, max_e)
        if self.temp is not None and \
            self.beacon.model_temp is not None:
            freq = self.beacon.model_temp.compensate(
                            freq, self.temp, temp)
        return freq

class BeaconTempModelBuilder:
    _DEFAULTS = {'amfg': 1.0,
                'tcc': -2.1429828e-05,
                'tcfl': -1.8980091e-10,
                'tctl': 3.6738370e-16,
                'fmin' : None,
                'fmin_temp' : None}

    @classmethod
    def load(cls, config):
        return BeaconTempModelBuilder(config)

    def __init__(self, config):
        self.parameters = BeaconTempModelBuilder._DEFAULTS.copy()
        for key in self.parameters.keys():
            param = config.getfloat('tc_' + key, None)
            if param is not None:
                self.parameters[key] = param

    def build(self):
        if self.parameters['fmin'] is None or \
            self.parameters['fmin_temp'] is None:
            return None
        logging.info('beacon: built tempco model %s', self.parameters)
        return BeaconTempModel(**self.parameters)

    def build_with_nvm(self, beacon):
        nvm_data = beacon.beacon_nvm_read_cmd.send([6, 0])
        (f_count, adc_count) = struct.unpack("<IH", nvm_data['bytes'])
        if f_count < 0xFFFFFFFF and adc_count < 0xFFFF:
            if self.parameters['fmin'] is None:
                self.parameters['fmin'] = beacon.count_to_freq(f_count)
                logging.info("beacon: loaded fmin=%.2f from nvm",
                    self.parameters['fmin'])
            if self.parameters['fmin_temp'] is None:
                temp_adc = float(adc_count) / beacon.temp_smooth_count * \
                    beacon.inv_adc_max
                self.parameters['fmin_temp'] = \
                    beacon.thermistor.calc_temp(temp_adc)
                logging.info("beacon: loaded fmin_temp=%.2f from nvm",
                    self.parameters['fmin_temp'])
        else:
            logging.info("beacon: fmin parameters not found in nvm")
        return self.build()

class BeaconTempModel:
    def __init__(self, amfg, tcc, tcfl, tctl, fmin, fmin_temp):
        self.amfg = amfg
        self.tcc = tcc
        self.tcfl = tcfl
        self.tctl = tctl
        self.fmin = fmin
        self.fmin_temp = fmin_temp

    def _tcf(self, f, df, dt, tctl):
        tctl = self.tctl if tctl is None else tctl
        tc = self.tcc + self.tcfl * df + tctl * df * df
        return f + self.amfg * tc * dt * f

    def compensate(self, freq, temp_source, temp_target, tctl=None):
        dt = temp_target - temp_source
        dfmin = self.fmin * self.amfg * self.tcc * \
                (temp_source - self.fmin_temp)
        df = freq - (self.fmin + dfmin)
        if dt < 0.:
            f2 = self._tcf(freq, df, dt, tctl)
            dfmin2 = self.fmin * self.amfg * self.tcc * \
                    (temp_target - self.fmin_temp)
            df2 = f2 - (self.fmin + dfmin2)
            f3 = self._tcf(f2, df2, -dt, tctl)
            ferror = freq - f3
            freq = freq + ferror
            df = freq - (self.fmin + dfmin)
        return self._tcf(freq, df, dt, tctl)

class ModelManager:
    def __init__(self, beacon):
        self.beacon = beacon
        self.gcode = beacon.printer.lookup_object('gcode')
        self.gcode.register_command('BEACON_MODEL_SELECT',
                                    self.cmd_BEACON_MODEL_SELECT,
                                    desc=self.cmd_BEACON_MODEL_SELECT_help)
        self.gcode.register_command('BEACON_MODEL_SAVE',
                                    self.cmd_BEACON_MODEL_SAVE,
                                    desc=self.cmd_BEACON_MODEL_SAVE_help)
        self.gcode.register_command('BEACON_MODEL_REMOVE',
                                    self.cmd_BEACON_MODEL_REMOVE,
                                    desc=self.cmd_BEACON_MODEL_REMOVE_help)
        self.gcode.register_command('BEACON_MODEL_LIST',
                                    self.cmd_BEACON_MODEL_LIST,
                                    desc=self.cmd_BEACON_MODEL_LIST_help)

    cmd_BEACON_MODEL_SELECT_help = "Load named beacon model"
    def cmd_BEACON_MODEL_SELECT(self, gcmd):
        name = gcmd.get("NAME")
        model = self.beacon.models.get(name, None)
        if model is None:
            raise gcmd.error("Unknown model '%s'" % (name,))
        self.beacon.model = model
        gcmd.respond_info("Selected Beacon model '%s'" % (name,))

    cmd_BEACON_MODEL_SAVE_help = "Save current beacon model"
    def cmd_BEACON_MODEL_SAVE(self, gcmd):
        model = self.beacon.model
        if model is None:
            raise gcmd.error("No model currently selected")
        oldname = model.name
        name = gcmd.get("NAME", oldname)
        if name != oldname:
            model = copy.copy(model)
        model.name = name
        model.save(self.beacon)
        if name != oldname:
            self.beacon.models[name] = model

    cmd_BEACON_MODEL_REMOVE_help = "Remove saved beacon model"
    def cmd_BEACON_MODEL_REMOVE(self, gcmd):
        name = gcmd.get("NAME")
        model = self.beacon.models.get(name, None)
        if model is None:
            raise gcmd.error("Unknown model '%s'" % (name,))
        configfile = self.beacon.printer.lookup_object('configfile')
        section = "beacon model " + model.name
        configfile.remove_section(section)
        self.beacon.models.pop(name)
        gcmd.respond_info("Model '%s' was removed for the current session.\n"
                          "Run SAVE_CONFIG to update the printer configuration"
                          "and restart Klipper." % (name,))
        if self.beacon.model == model:
            self.beacon.model = None

    cmd_BEACON_MODEL_LIST_help = "Remove saved beacon model"
    def cmd_BEACON_MODEL_LIST(self, gcmd):
        if not self.beacon.models:
            gcmd.respond_info("No Beacon models loaded")
            return
        gcmd.respond_info("List of loaded Beacon models:")
        current_model = self.beacon.model
        for _name, model in sorted(self.beacon.models.items()):
            if model == current_model:
                gcmd.respond_info("- %s [active]" % (model.name,))
            else:
                gcmd.respond_info("- %s" % (model.name,))


class AlphaBetaFilter:
    def __init__(self, alpha, beta):
        self.alpha = alpha
        self.beta = beta
        self.reset()

    def reset(self):
        self.xl = None
        self.vl = 0
        self.tl = None

    def update(self, time, measurement):
        if self.xl == None:
            self.xl = measurement
        if self.tl is not None:
            dt = time - self.tl
        else:
            dt = 0
        self.tl = time
        xk = self.xl + self.vl * dt
        vk = self.vl
        rk = measurement - xk
        xk = xk + self.alpha * rk
        if dt > 0:
            vk = vk + self.beta / dt * rk
        self.xl = xk
        self.vl = vk
        return xk

    def value(self):
        return self.xl

class StreamingHelper:
    def __init__(self, beacon, callback, completion_callback, latency):
        self.beacon = beacon
        self.cb = callback
        self.completion_cb = completion_callback
        self.completion = self.beacon.reactor.completion()

        self.latency_key = None
        if latency is not None:
            self.latency_key = self.beacon.request_stream_latency(latency)

        self.beacon._stream_callbacks[self] = self._handle
        self.beacon._start_streaming()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def _handle(self, sample):
        try:
            self.cb(sample)
        except StopStreaming:
            self.completion.complete(())

    def stop(self):
        if not self in self.beacon._stream_callbacks:
            return
        del self.beacon._stream_callbacks[self]
        self.beacon._stop_streaming()
        if self.latency_key is not None:
            self.beacon.drop_stream_latency_request(self.latency_key)
        if self.completion_cb is not None:
            self.completion_cb()

    def wait(self):
        self.completion.wait()
        self.stop()

class StopStreaming(Exception):
    pass


class APIDumpHelper:
    def __init__(self, beacon):
        self.beacon = beacon
        self.clients = {}
        self.stream = None
        self.buffer = []
        self.fields = ["dist", "temp", "pos", "freq", "vel", "time"]

    def _start_stop(self):
        if not self.stream and self.clients:
            self.stream = self.beacon.streaming_session(self._cb)
        elif self.stream is not None and not self.clients:
            self.stream.stop()
            self.stream = None

    def _cb(self, sample):
        tmp = [sample.get(key, None) for key in self.fields]
        self.buffer.append(tmp)
        if len(self.buffer) > 50:
            self._update_clients()

    def _update_clients(self):
        for cconn, template in list(self.clients.items()):
            if cconn.is_closed():
                del self.clients[cconn]
                self._start_stop()
                continue
            tmp = dict(template)
            tmp['params'] = self.buffer
            cconn.send(tmp)
        self.buffer = []

    def add_client(self, web_request):
        cconn = web_request.get_client_connection()
        template = web_request.get_dict('response_template', {})
        self.clients[cconn] = template
        self._start_stop()
        web_request.send({'header': self.fields})

class BeaconProbeWrapper:
    def __init__(self, beacon):
        self.beacon = beacon

    def multi_probe_begin(self):
        return self.beacon.multi_probe_begin()
    def multi_probe_end(self):
        return self.beacon.multi_probe_end()
    def get_offsets(self):
        return self.beacon.get_offsets()
    def get_lift_speed(self, gcmd=None):
        return self.beacon.get_lift_speed(gcmd)
    def run_probe(self, gcmd):
        return self.beacon.run_probe(gcmd)

class BeaconTempWrapper:
    def __init__(self, beacon):
        self.beacon = beacon

    def get_temp(self, eventtime):
        return self.beacon.last_temp, 0

    def get_status(self, eventtime):
        return {
            'temperature': round(self.beacon.last_temp, 2),
            'measured_min_temp': round(self.beacon.measured_min, 2),
            'measured_max_temp': round(self.beacon.measured_max, 2)
        }

TRSYNC_TIMEOUT = 0.025
TRSYNC_SINGLE_MCU_TIMEOUT = 0.250

class BeaconEndstopWrapper:
    def __init__(self, beacon):
        self.beacon = beacon
        self._mcu = beacon._mcu

        ffi_main, ffi_lib = chelper.get_ffi()
        self._trdispatch = ffi_main.gc(ffi_lib.trdispatch_alloc(), ffi_lib.free)
        self._trsyncs = [MCU_trsync(self.beacon._mcu, self._trdispatch)]

        printer = self.beacon.printer
        printer.register_event_handler('klippy:mcu_identify',
                                       self._handle_mcu_identify)
        printer.register_event_handler('homing:home_rails_end',
                                       self._handle_home_rails_end)

        self.z_homed = False

    def _handle_mcu_identify(self):
        self.toolhead = self.beacon.printer.lookup_object("toolhead")
        kin = self.toolhead.get_kinematics()
        for stepper in kin.get_steppers():
            if stepper.is_active_axis('z'):
                self.add_stepper(stepper)

    def _handle_home_rails_end(self, homing_state, rails):
        if self.beacon.model is None:
            return

        if 2 not in homing_state.get_axes():
            return

        # After homing Z we perform a measurement and adjust the toolhead
        # kinematic position.
        samples = self.beacon._sample_printtime_sync(5, 10)
        dist = median([s['dist'] for s in samples])
        homing_state.set_homed_position([None, None, dist])

    def get_mcu(self):
        return self._mcu

    def add_stepper(self, stepper):
        trsyncs = {trsync.get_mcu(): trsync for trsync in self._trsyncs}
        stepper_mcu = stepper.get_mcu()
        trsync = trsyncs.get(stepper_mcu)
        if trsync is None:
            trsync = MCU_trsync(stepper_mcu, self._trdispatch)
            self._trsyncs.append(trsync)
        trsync.add_stepper(stepper)
        # Check for unsupported multi-mcu shared stepper rails, duplicated
        # from MCU_endstop
        sname = stepper.get_name()
        if sname.startswith('stepper_'):
            for ot in self._trsyncs:
                for s in ot.get_steppers():
                    if ot is not trsync and s.get_name().startswith(sname[:9]):
                        cerror = self._mcu.get_printer().config_error
                        raise cerror("Multi-mcu homing not supported on"
                                     " multi-mcu shared axis")

    def get_steppers(self):
        return [s for trsync in self._trsyncs for s in trsync.get_steppers()]

    def home_start(self, print_time, sample_time, sample_count, rest_time,
                   triggered=True):
        if self.beacon.model is None:
            raise self.beacon.printer.command_error("No Beacon model loaded")

        self.beacon._apply_threshold()
        clock = self._mcu.print_time_to_clock(print_time)
        rest_ticks = self._mcu.print_time_to_clock(print_time+rest_time) - clock
        self._rest_ticks = rest_ticks
        reactor = self._mcu.get_printer().get_reactor()
        self._trigger_completion = reactor.completion()
        expire_timeout = TRSYNC_TIMEOUT
        if len(self._trsyncs) == 1:
            expire_timeout = TRSYNC_SINGLE_MCU_TIMEOUT
        for trsync in self._trsyncs:
            trsync.start(print_time, self._trigger_completion, expire_timeout)
        etrsync = self._trsyncs[0]
        ffi_main, ffi_lib = chelper.get_ffi()
        ffi_lib.trdispatch_start(self._trdispatch, etrsync.REASON_HOST_REQUEST)
        self.beacon.beacon_home_cmd.send([
            etrsync.get_oid(),
            etrsync.REASON_ENDSTOP_HIT,
            0,
        ])
        return self._trigger_completion

    def home_wait(self, home_end_time):
        etrsync = self._trsyncs[0]
        etrsync.set_home_end_time(home_end_time)
        if self._mcu.is_fileoutput():
            self._trigger_completion.complete(True)
        self._trigger_completion.wait()
        self.beacon.beacon_stop_home.send()
        ffi_main, ffi_lib = chelper.get_ffi()
        ffi_lib.trdispatch_stop(self._trdispatch)
        res = [trsync.stop() for trsync in self._trsyncs]
        if any([r == etrsync.REASON_COMMS_TIMEOUT for r in res]):
            return -1.
        if res[0] != etrsync.REASON_ENDSTOP_HIT:
            return 0.
        if self._mcu.is_fileoutput():
            return home_end_time
        return home_end_time

    def query_endstop(self, print_time):
        if self.beacon.model is None:
            return 1
        clock = self._mcu.print_time_to_clock(print_time)
        sample = self.beacon._sample_async()
        if self.beacon.trigger_freq <= sample['freq']:
            return 1
        else:
            return 0

    def get_position_endstop(self):
        return self.beacon.trigger_distance

class BeaconMeshHelper:
    @classmethod
    def create(cls, beacon, config):
        if config.has_section('bed_mesh'):
            return BeaconMeshHelper(beacon, config)
        else:
            return None

    def __init__(self, beacon, config):
        self.beacon = beacon
        mesh_config = self.mesh_config = config.getsection('bed_mesh')
        self.bm = self.beacon.printer.load_object(mesh_config, 'bed_mesh')

        self.speed = mesh_config.getfloat('speed', 50., above=0.,
                                          note_valid=False)
        self.min_x, self.min_y = mesh_config.getfloatlist('mesh_min',
            count=2, note_valid=False)
        self.max_x, self.max_y = mesh_config.getfloatlist('mesh_max',
            count=2, note_valid=False)
        self.res_x, self.res_y = mesh_config.getintlist('probe_count',
            count=2, note_valid=False)
        self.rri = mesh_config.getint('relative_reference_index', None,
            note_valid=False)
        self.dir = config.getchoice('mesh_main_direction',
                {'x': 'x', 'X': 'x', 'y': 'y', 'Y': 'y'}, 'y')
        self.overscan = config.getfloat('mesh_overscan', -1, minval=0)
        self.cluster_size = config.getfloat('mesh_cluster_size', 1, minval=0)
        self.runs = config.getint('mesh_runs', 1, minval=1)

        self.step_x = (self.max_x - self.min_x) / (self.res_x - 1)
        self.step_y = (self.max_y - self.min_y) / (self.res_y - 1)

        self.faulty_regions = []
        for i in list(range(1, 100, 1)):
            start = mesh_config.getfloatlist("faulty_region_%d_min" % (i,), None,
                                        count=2)
            if start is None:
                break
            end = mesh_config.getfloatlist("faulty_region_%d_max" % (i,), count=2)
            x_min = min(start[0], end[0])
            x_max = max(start[0], end[0])
            y_min = min(start[1], end[1])
            y_max = max(start[1], end[1])
            self.faulty_regions.append(Region(x_min, x_max, y_min, y_max))

        self.gcode = self.beacon.printer.lookup_object('gcode')
        self.prev_gcmd = self.gcode.register_command('BED_MESH_CALIBRATE', None)
        self.gcode.register_command(
            'BED_MESH_CALIBRATE', self.cmd_BED_MESH_CALIBRATE,
            desc=self.cmd_BED_MESH_CALIBRATE_help)

        if self.overscan < 0:
            printer = self.beacon.printer
            printer.register_event_handler('klippy:mcu_identify',
                                           self._handle_mcu_identify)

    cmd_BED_MESH_CALIBRATE_help = "Perform Mesh Bed Leveling"
    def cmd_BED_MESH_CALIBRATE(self, gcmd):
        method = gcmd.get('METHOD', 'beacon').lower()
        if method == 'beacon':
            self.calibrate(gcmd)
        else:
            self.prev_gcmd(gcmd)

    def _handle_mcu_identify(self):
        # Auto determine a safe overscan amount
        toolhead = self.beacon.printer.lookup_object("toolhead")
        curtime = self.beacon.reactor.monotonic()
        status = toolhead.get_kinematics().get_status(curtime)
        xo = self.beacon.x_offset
        yo = self.beacon.y_offset
        settings = {
            'x': {
                'range': [self.min_x-xo, self.max_x-xo],
                'machine': [status['axis_minimum'][0],
                            status['axis_maximum'][0]],
                'count': self.res_y,
            },
            'y': {
                'range': [self.min_y-yo, self.max_y-yo],
                'machine': [status['axis_minimum'][1],
                            status['axis_maximum'][1]],
                'count': self.res_x,
            }
        }[self.dir]

        r = settings['range']
        m = settings['machine']
        space = (r[1] - r[0]) / (float(settings['count']-1))
        self.overscan = min([
            max(0, r[0]-m[0]),
            max(0, m[1]-r[1]),
            space+2.0, # A half circle with 2mm lead in/out
        ])

    def _generate_path(self):
        xo = self.beacon.x_offset
        yo = self.beacon.y_offset
        settings = {
            'x': {
                'range_aligned': [self.min_x-xo, self.max_x-xo],
                'range_perpendicular': [self.min_y-yo, self.max_y-yo],
                'count': self.res_y,
                'swap_coord': False,
            },
            'y': {
                'range_aligned': [self.min_y-yo, self.max_y-yo],
                'range_perpendicular': [self.min_x-xo, self.max_x-xo],
                'count': self.res_x,
                'swap_coord': True,
            }
        }[self.dir]

        # We build the path in "normalized" coordinates and then simply
        # swap x and y at the end if we need to
        begin_a, end_a = settings['range_aligned']
        begin_p, end_p = settings['range_perpendicular']
        swap_coord = settings['swap_coord']
        step = (end_p - begin_p) / (float(settings['count']-1))
        points = []
        corner_radius = min(step/2, self.overscan)
        for i in range(0, settings['count']):
            pos_p = begin_p + step * i
            even = i % 2 == 0 # If even we are going 'right', else 'left'
            pa = (begin_a, pos_p) if even else (end_a, pos_p)
            pb = (end_a, pos_p) if even else (begin_a, pos_p)

            l = (pa,pb)

            if len(points) > 0 and corner_radius > 0:
                # We need to insert an overscan corner. Basically we insert
                # a rounded rectangle to smooth out the transition and retain
                # as much speed as we can.
                #
                #  ---|---<
                # /
                # |
                # \
                #  ---|--->
                #
                # We just need to draw the two 90 degree arcs. They contain
                # the endpoints of the lines connecting everything.
                if even:
                    center = begin_a - self.overscan + corner_radius
                    points += arc_points(center, pos_p - step + corner_radius,
                            corner_radius, -90, -90)
                    points += arc_points(center, pos_p - corner_radius,
                            corner_radius, -180, -90)
                else:
                    center = end_a + self.overscan - corner_radius
                    points += arc_points(center, pos_p - step + corner_radius,
                            corner_radius, -90, 90)
                    points += arc_points(center, pos_p - corner_radius,
                            corner_radius, 0, 90)

            points.append(l[0])
            points.append(l[1])

        if swap_coord:
            for i in range(len(points)):
                (x,y) = points[i]
                points[i] = (y,x)

        return points

    def calibrate(self, gcmd):
        self.toolhead = self.beacon.toolhead
        path = self._generate_path()

        probe_speed = gcmd.get_float("PROBE_SPEED", self.beacon.speed, above=0.)
        self.beacon._move_to_probing_height(probe_speed)

        speed = gcmd.get_float("SPEED", self.speed, above=0.)
        runs = gcmd.get_int("RUNS", self.runs, minval=1)

        try:
            self.beacon._start_streaming()

            # Move to first location
            (x,y) = path[0]
            self.toolhead.manual_move([x, y, None], speed)
            self.toolhead.wait_moves()

            self.beacon._sample_printtime_sync(5)
            clusters = self._sample_mesh(gcmd, path, speed, runs)

        finally:
            self.beacon._stop_streaming()

        clusters = self._interpolate_faulty(clusters)
        self._apply_mesh(clusters, gcmd)

    def _fly_path(self, path, speed, runs):
        # Run through the path
        for i in range(runs):
            p = path if i % 2 == 0 else reversed(path)
            for (x,y) in p:
                self.toolhead.manual_move([x, y, None], speed)
        self.toolhead.wait_moves()

    def _sample_mesh(self, gcmd, path, speed, runs):
        cs = gcmd.get_float("CLUSTER_SIZE", self.cluster_size, minval=0.)

        min_x, min_y = self.min_x, self.min_y
        xo, yo = self.beacon.x_offset, self.beacon.y_offset

        clusters = {}
        total_samples = [0]

        def cb(sample):
            total_samples[0] += 1

            (x, y, z) = sample['pos']
            x += xo
            y += yo
            d = sample['dist']

            # Calculate coordinate of the cluster we are in
            xi = int(round((x - min_x) / self.step_x))
            yi = int(round((y - min_y) / self.step_y))

            # If there's a cluster size limit, apply it here
            if cs > 0:
                xf = xi * self.step_x + min_x
                yf = yi * self.step_y + min_y
                dx = x - xf
                dy = y - yf
                dist = math.sqrt(dx*dx+dy*dy)
                if dist > cs:
                    return

            k = (xi, yi)

            if k not in clusters:
                clusters[k] = []
            clusters[k].append(d)

        with self.beacon.streaming_session(cb) as ss:
            self._fly_path(path, speed, runs)

        gcmd.respond_info("Sampled %d total points over %d runs" %
                          (total_samples[0], runs))
        gcmd.respond_info("Samples binned in %d clusters" % (len(clusters),))

        return clusters

    def _is_faulty_coordinate(self, x, y):
        for r in self.faulty_regions:
            if r.is_point_within(x, y):
                return True
        return False

    def _interpolate_faulty(self, clusters):
        faulty_indexes = []
        xi_max = 0
        yi_max = 0
        for (xi, yi), points in clusters.items():
            if xi > xi_max:
                xi_max = xi
            if yi > yi_max:
                yi_max = yi
            xc = xi * self.step_x + self.min_x
            yc = yi * self.step_y + self.min_y
            if self._is_faulty_coordinate(xc, yc):
                clusters[(xi, yi)] = None
                faulty_indexes.append((xi, yi))

        def get_nearest(start, dx, dy):
            (x, y) = start
            x += dx
            y += dy
            while (x >= 0 and x <= xi_max and
                   y >= 0 and y <= yi_max):
                if clusters[(x, y)] is not None:
                    return (abs(x-start[0])+abs(y-start[0]), median(clusters[(x,y)]))
                x += dx
                y += dy
            return None

        def interp_weighted(lower, higher):
            if lower is None and higher is None:
                return None
            if lower is None and higher is not None:
                return higher[1]
            elif lower is not None and higher is None:
                return lower[1]
            else:
                return ((lower[1] * lower[0] + higher[1] * higher[0]) /
                        (lower[0] + higher[0]))

        for coord in faulty_indexes:
            xl = get_nearest(coord, -1,  0)
            xh = get_nearest(coord,  1,  0)
            xavg = interp_weighted(xl, xh)
            yl = get_nearest(coord,  0, -1)
            yh = get_nearest(coord,  0,  1)
            yavg = interp_weighted(yl, yh)
            avg = None
            if xavg is not None and yavg is None:
                avg = xavg
            elif xavg is None and yavg is not None:
                avg = yavg
            else:
                avg = (xavg + yavg) / 2.0
            clusters[coord] = [avg]

        return clusters

    def _apply_mesh(self, clusters, gcmd):
        matrix = []
        td = self.beacon.trigger_distance
        for yi in range(self.res_y):
            line = []
            for xi in range(self.res_x):
                cluster = clusters.get((xi,yi), None)
                if cluster is None or len(cluster) == 0:
                    xc = xi * self.step_x + self.min_x
                    yc = yi * self.step_y + self.min_y
                    logging.info("Cluster (%.3f,%.3f)[%d,%d] is empty!"
                                 % (xc, yc,
                                    xi, yi))
                    err = ("Empty clusters found\n"
                           "Try increasing mesh cluster_size or slowing down")
                    raise self.gcode.error(err)
                data = [td-d for d in cluster]
                line.append(median(data))
            matrix.append(line)

        rri = gcmd.get_int('RELATIVE_REFERENCE_INDEX', self.rri)
        if rri is not None:
            if rri < 0 or rri >= self.res_x * self.res_y:
                rri = None

        if rri is not None:
            rri_x = rri % self.res_x
            rri_y = int(math.floor(rri / self.res_x))
            z_offset = matrix[rri_y][rri_x]
            for i, line in enumerate(matrix):
                matrix[i] = [z-z_offset for z in line]

        params = self.bm.bmc.mesh_config
        params['min_x'] = self.min_x
        params['max_x'] = self.max_x
        params['min_y'] = self.min_y
        params['max_y'] = self.max_y
        params['x_count'] = self.res_x
        params['y_count'] = self.res_y
        mesh = bed_mesh.ZMesh(params)
        try:
            mesh.build_mesh(matrix)
        except bed_mesh.BedMeshError as e:
            raise self.gcode.error(str(e))
        self.bm.set_mesh(mesh)
        self.gcode.respond_info("Mesh calibration complete")
        self.bm.save_profile(gcmd.get('PROFILE', "default"))

class Region:
    def __init__(self, x_min, x_max, y_min, y_max):
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max

    def is_point_within(self, x, y):
        return ((x > self.x_min and x < self.x_max) and
                (y > self.y_min and y < self.x_max))

def arc_points(cx, cy, r, start_angle, span):
    # Angle delta is determined by a max deviation(md) from 0.1mm:
    #   r * versin(d_a) < md
    #   versin(d_a) < md/r
    #   d_a < arcversin(md/r)
    #   d_a < arccos(1-md/r)
    # We then determine how many of these we can fit in exactly
    # 90 degrees(rounding up) and then determining the exact
    # delta angle.
    start_angle = start_angle / 180.0 * math.pi
    span = span / 180.0 * math.pi
    d_a = math.acos(1 - 0.1 / r)
    cnt = int(math.ceil(abs(span) / d_a))
    d_a = span / float(cnt)

    points = []
    for i in range(cnt+1):
        ang = start_angle + d_a*float(i)
        x = cx + math.cos(ang)*r
        y = cy + math.sin(ang)*r
        points.append((x,y))

    return points

def median(samples):
    return float(np.median(samples))

def load_config(config):
    beacon = BeaconProbe(config)
    config.get_printer().add_object('probe', BeaconProbeWrapper(beacon))
    temp = BeaconTempWrapper(beacon)
    config.get_printer().add_object('temperature_sensor beacon_coil', temp)
    pheaters = beacon.printer.load_object(config, 'heaters')
    pheaters.available_sensors.append('temperature_sensor beacon_coil')
    return beacon

def load_config_prefix(config):
    beacon = config.get_printer().lookup_object('beacon')
    name = config.get_name()
    if name.startswith('beacon model '):
        name = name[13:]
        model = BeaconModel.load(name, config, beacon)
        beacon._register_model(name, model)
        return model
    else:
        raise config.error("Unknown beacon config directive '%s'" % (name[7:],))
