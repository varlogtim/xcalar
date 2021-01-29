#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`
XLRROOT=$(readlink -f $DIR/../../..)
# Prioritize the out-of-source build location; fallback to in-source

numNodes=$1
Profile=${2:-default}
MemDebugger=${3:-none}
nodeId=${4:-$numNodes}
SpawnMgmtd=${SpawnMgmtd:-true}
ExpServerd=${ExpServerd:-true}
SpawnSqldf=${SpawnSqldf:-true}
TERMINAL=${TERMINAL:-xterm}

if [ -n "$XLRGUIDIR" ]; then
    XLRGUIROOT="$XLRGUIDIR"
else
    XLRGUIROOT="$(readlink -f $XLRROOT/../xcalar-gui)"
fi

if [ "$ExpServerd" = "true" ]; then
    if [ ! -d "$XLRGUIROOT" ]; then
        echo "failed to find XLRGUIROOT" 1>&2
        exit 1
    fi
fi

STRESSTEST=-1

if [ $# -lt "1" -o $# -gt "4" ]; then
    echo "Usage: $0 <numNodes> [silent|daemon|debug] [none|valgrind|valgrindtrack|guardrails] <nodeId>"
    exit 0
fi

TMP1=`mktemp /tmp/launcher.XXXXX`
rm -rf $TMP1
mkdir $TMP1

Hostname=`hostname`
export XCE_CONFIG="${XCE_CONFIG:-$(readlink -f $DIR/test-config.cfg)}"
export XCE_LICENSEDIR="${XCE_LICENSEDIR:-$XLRDIR/src/data}"
export XCE_LICENSEFILE="${XCE_LICENSEFILE:-$XCE_LICENSEDIR/XcalarLic.key}"

logdir="$(awk -F'=' '/^Constants.XcalarLogCompletePath/{print $2}' $XCE_CONFIG)"
logdir=${logdir:-/var/log/xcalar}

xlrRoot="$(awk -F'=' '/^Constants.XcalarRootCompletePath/{print $2}' $XCE_CONFIG)"
xlrRoot=${xlrRoot:-/var/opt/xcalar}

echo "Config file: $XCE_CONFIG"
echo "Output directory: $TMP1"

if [ ! -w $logdir ]; then
    echo "logdir $logdir not accessible for writing!" 1>&2
    echo "will log to stderr!" 1>&2
else
    echo "Xcalar Logs directory: $logdir"
fi

if [ ! -d $xlrRoot ] || [ ! -w $xlrRoot ]; then
    echo "xlrRoot $xlrRoot does not exist or is not accessible for writing!" 1>&2
    exit 1
else
    echo "Xcalar Root directory: $xlrRoot"
fi

mkdir -p "${xlrRoot}/config"

if [ ! -w "${xlrRoot}/config" ]; then
    echo "config directory ${xlrRoot}/config is not accessible for writing!" 1>&2
    exit 1
fi

if [ -f "${xlrRoot}/config/defaultAdmin.json" ]; then
    configPerms="$(stat -c '%a' "${xlrRoot}/config/defaultAdmin.json")"
    if [ ! -r "${xlrRoot}/config/defaultAdmin.json" ]; then
        echo "Default admin config file ${xlrRoot}/config/defaultAdmin.json cannot be read" 1>&2
        exit 1
    elif [ "$configPerms" != "600" ]; then
        echo "Permissions of admin file ${xlrRoot}/config/defaultAdmin.json are $configPerms; they must be 600" 1>&2
        exit 1
    fi
else
    ${XLRROOT}/pkg/gui-installer/default-admin.py -e "support@xcalar.com" -u admin -p admin > ${xlrRoot}/config/defaultAdmin.json && chmod 0600 ${xlrRoot}/config/defaultAdmin.json
fi

echo "Setting up .jupyter, .ipython, .local, and jupyterNotebooks directories"
mkdir -p ~/.local
JUPYTER_DIR=~/.jupyter
test -e $JUPYTER_DIR -a ! -h $JUPYTER_DIR && rm -rf $JUPYTER_DIR
ln -sfn ${XLRGUIROOT}/xcalar-gui/assets/jupyter/jupyter $JUPYTER_DIR
IPYTHON_DIR=~/.ipython
test -e $IPYTHON_DIR -a ! -h $IPYTHON_DIR && rm -rf $IPYTHON_DIR
ln -sfn ${XLRGUIROOT}/xcalar-gui/assets/jupyter/ipython $IPYTHON_DIR
mkdir -p "${xlrRoot}/jupyterNotebooks"

interactiveMode=0
if [ "$Profile" = "debug" ]; then
    interactiveMode=1
fi

if [ "$Profile" = "silent" -o "$Profile" = "daemon" -o "$Profile" = "debug" ]
then

    # Use functions located in $XLRDIR/bin/nodes.sh.
    . nodes.sh

    murderNodes

    if [ "$SpawnMgmtd" = "true" ]; then
        spawnMgmtd "$XLRROOT" "$TMP1/xcmgmtd.out"
        ret=$?
        MGMTPID=$output
        if [ "$ret" != "0" ] || [ "$MGMTPID" = "0" ]
        then
            printInColor "red" "Failed to spawn xcmgmtd" 1>&2
            exit 1
        fi
    fi
    if [ "$ExpServerd" = "true" ]; then
        spawnExpServer "$XLRGUIROOT" "$TMP1/expServer.out"
        ret=$?
        EXPSVRPID=$output
        if [ "$ret" != "0" ] || [ "$EXPSVRPID" = "0" ]; then
            printInColor "red" "Failed to spawn expServer" 1>&2
            exit 1
        fi
    fi

    if [ "$SpawnSqldf" = "true" ]; then
        spawnSqldf "$TMP1/sqldf.out"
        ret=$?
        SQLDFPID=$output
        if [ "$ret" != "0" ] || [ "$SQLDFPID" = "0" ]; then
            printInColor "yellow" "Failed to spawn xcalar-sqldf" 1>&2
            # non-fatal for now
        fi
    fi

    if (( $nodeId == $numNodes )); then
        spawnXcMonitors "$numNodes" "$TMP1" "$MemDebugger" "$XCE_LICENSEFILE"
    else
        spawnXcMonitor "$nodeId" "$numNodes" "$TMP1" "$MemDebugger" "$XCE_LICENSEFILE"
    fi
    ret=$?
    if [ "$ret" != "0" ]; then
        echo "Failed to spawn xcmonitors" 1>&2
        exit 1
    fi
    if [ "$Profile" != "daemon" -a "$Profile" != "debug" ]; then
        waitForNodes
        if [ "$?" != "0" ]
        then
            echo "*** Nonzero usrnode exit value detected!" 1>&2
            echo "*** leaving $TMP1 around for you to examine" 1>&2
            echo "*** Also check $logdir for the Xcalar logs" 1>&2
        else
            rm -rf $TMP1
        fi
    fi
else
    set -x
    if [ "$SpawnMgmtd" = "true" -a "$Profile" != "stress" ]; then
        cd $XLRROOT/buildOut/src/bin/mgmtd
        pwd
        if [ "$TERMINAL" = "gnome-terminal" ]; then
            $TERMINAL --tab --profile $Profile -e "./xcmgmtd $XCE_CONFIG" &
        elif [ "$TERMINAL" = "lxterminal" ]; then
            $TERMINAL --title="xcmgmtd" --command="./xcmgmtd $XCE_CONFIG" &
        else
            $TERMINAL -title "xcmgmtd" -hold -sb -sl 1000000 -bg white -fg black -e /bin/bash -l -c "./xcmgmtd $XCE_CONFIG" &
        fi
    fi
    if [ "$Profile" != "stress" ]; then
        cd $XLRROOT/buildOut/src/bin/monitor
        pwd
        if (( $nodeId == $numNodes )); then
            for i in `seq 1 $numNodes`;
            do
                nodeId=$(($i - 1))
                xcMonitorString="$XLRROOT/buildOut/src/bin/monitor/xcmonitor -n $nodeId -m $numNodes -c $XCE_CONFIG"
                if [ "$TERMINAL" = "gnome-terminal" ]; then
                    $TERMINAL --tab --profile $Profile -e "$xcMonitorString" &
                elif [ "$TERMINAL" = "lxterminal" ]; then
                    $TERMINAL --title="xcmonitor" --command="$xcMonitorString" &
                else
                    $TERMINAL -title "xcmonitor" -hold -sb -sl 1000000 -bg white -fg black -e /bin/bash -l -c "$xcMonitorString" &
                fi
            done
        else
            xcMonitorString="$XLRROOT/buildOut/src/bin/monitor/xcmonitor -n $nodeId -m $numNodes -c $XCE_CONFIG"
            if [ "$TERMINAL" = "gnome-terminal" ]; then
                $TERMINAL --tab --profile $Profile -e "$xcMonitorString" &
            elif [ "$TERMINAL" = "lxterminal" ]; then
                $TERMINAL --title="xcmonitor" --command="$xcMonitorString" &
            else
                $TERMINAL -title "xcmonitor" -hold -sb -sl 1000000 -bg white -fg black -e /bin/bash -l -c "$xcMonitorString" &
            fi
        fi
    fi
    if [ "$ExpServerd" = "true" -a "$Profile" != "stress" ]; then
        cd $XLRGUIROOT/xcalar-gui/services/expServer
        pwd
        if [ "$TERMINAL" = "gnome-terminal" ]; then
            $TERMINAL --tab --profile $Profile -e "npm start" &
        elif [ "$TERMINAL" = "lxterminal" ]; then
            $TERMINAL --title="expServer" --command="npm start" &
        else
            $TERMINAL -title "expServer" -hold -sb -sl 1000000 -bg white -fg black -e /bin/bash -l -c "npm start" &
        fi
    fi
fi
