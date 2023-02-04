#!/bin/bash

KDIR="${HOME}/klipper"
KENV="${HOME}/klippy-env"

BKDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

if [ ! -d "$KDIR" ] || [ ! -d "$KENV" ]; then
    echo "klipper or klippy env doesn't exist"
    exit 1
fi

# install beacon requirements to env
"${KENV}/bin/pip" install -r requirements.txt

# update link to beacon.py
if [ -e "${KDIR}/klippy/extras/beacon.py" ]; then
    rm "${KDIR}/klippy/extras/beacon.py"
fi
ln -s "${BKDIR}/beacon.py" "${KDIR}/klippy/extras/beacon.py"

# exclude beacon.py from klipper git tracking
if ! grep -q "klippy/extras/beacon.py" "${KDIR}/.git/info/exclude"; then
    echo "klippy/extras/beacon.py" >> "${KDIR}/.git/info/exclude"
fi
