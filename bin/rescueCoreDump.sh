#!/bin/bash

# This file takes, as input, a .crash file produced by apport and produces a
# usable core dump file. Should be run on uncompressed .crash files.
# Internal use only.

DIR="$(cd "$(dirname "$0")"&& pwd)"

dpkg -l | grep -q apport-retrace
if [ "$?" != "0" ]
then
    echo "Installing apport-retrace..."
    sudo apt-get -y install apport-retrace
fi

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: rescueCoreDump.sh <crash file> <destination>"
    exit 1
fi

# Continue in python. Ignore asserts with -O.
python -O "$DIR/rescueCoreDump.py" "$1" "$2"
