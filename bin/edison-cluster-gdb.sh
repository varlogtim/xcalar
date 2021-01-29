#!/bin/bash

for i in `seq 0 3`;
do
    gnome-terminal -e "gdb $XLRDIR/src/bin/usrnode/usrnode -ex 'target remote edison$i:12321'"
done
