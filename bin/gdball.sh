#!/bin/bash

XLRROOT="$(cd "$(dirname ${BASH_SOURCE[0]})"/.. && pwd)"
pathToUsrNode=$XLRROOT/src/bin/usrnode
pathToMgmtd=$XLRROOT/src/bin/mgmtd
TERMINAL=${TERMINAL:-xterm}

titleFlag="-T"
if [ "$TERMINAL" = "lxterminal" ] || [ "$TERMINAL" = "gnome-terminal" ]
then
    titleFlag="-t"
fi

cd $pathToUsrNode

if [ "$DISPLAY" = "" ]
then
    gdbCmd=$1
    if [ "$gdbCmd" = "" ]
    then
        echo "Not forking gdb sessions since \$DISPLAY is not set; you can run them manually as:"
    fi

    for pid in `pgrep -u \`whoami\` usrnode`
    do
        if [ "$gdbCmd" != "" ]
        then
            sudo -E gdb -p $pid ./usrnode -ex "set pagination off" -ex "$gdbCmd" -ex "detach" -ex "quit"
        else
            echo "sudo -E gdb -p $pid ./usrnode"
        fi

    done
    exit 0
fi

if [ "$1" = "coredump" ]
then
    pathToCoreDump=$2
    gdbCmd=$3
    gdb ./usrnode $pathToCoreDump -ex "set pagination off" -ex "$gdbCmd" -ex "quit"
else
    /bin/ps -e -o pid,args | grep usrnode | grep -v grep | grep $LOGNAME | while read pidline
    do
        pid=`echo $pidline | cut -f1 -d' '`
        nodeid=`echo $pidline | sed 's/^.*nodeId //' | cut -f1 -d' '`
        title="gdb usrnode (node $nodeid)"
        if [ "$1" = "" ]
        then
            # interactive
            $TERMINAL $titleFlag "$title" -e "sudo -E gdb -p $pid ./usrnode" &
        else
            echo $title
            sudo -E gdb -p $pid ./usrnode -ex "set pagination off" -ex "$*" -ex "quit"
        fi
    done

    cd $pathToMgmtd

    for pid in `pgrep -u \`whoami\` xcmgmtd`
    do
        title="gdb mgmtd"
        if [ "$1" = "" ]
        then
            # interactive
            $TERMINAL $titleFlag "$title" -e "sudo -E gdb -p $pid ./xcmgmtd" &
        else
            echo $title
            sudo -E gdb -p $pid ./usrnode -ex "set pagination off" -ex "$*" -ex "quit"
        fi
    done
fi
