#!/bin/bash

PATH="$XLRDIR/bin:$XLRDIR/scripts:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$XCE_USRNODE_PATH"
LD_LIBRARY_PATH="$XLRDIR/lib:$XLRDIR/lib64:$XCE_USRNODE_LD_LIB_PATH"

export PATH LD_LIBRARY_PATH

. determineNodeId.sh

determineNodeId

nodeIdArr=($nodeId)

apiPortNum=$(grep "^Node.${nodeIdArr[0]}.ApiPort" "$XCE_CONFIG" | cut -d '=' -f 2)

if [ -z "$apiPortNum" ]; then
    echo >&2 "Node.${nodeIdArr[0]}.ApiPort value not found.  Possibly outdated or corrupt config file (${XCE_CONFIG})."
    exit 1
fi

# check if Xcalar is running by checking a pid file and
# verify that the process there is running
isXcalarRunning() {

    if ! [ -f ${XCE_WORKDIR}/xcalar.pid ]; then
        return 1
    fi

    xcalarPid="$(cat ${XCE_WORKDIR}/xcalar.pid)"
    if [ -z "$xcalarPid" ] || ! [[ "$xcalarPid" =~ ^[0-9]+$ ]] ; then
        return 1
    fi

    if ! kill -0 $xcalarPid >/dev/null 2>&1; then
        return 1
    fi
    return 0
}

getCliBackendVersion() {
    cliBackendVersion=$(TERM=dumb $XLRDIR/bin/xccli -c "version" --port $1 | grep "Backend Version" | cut -d ':' -f2)
}

# the goal of this is to wait $XCE_USRNODE_WAIT seconds
# or until XCE responds to a version request
# this does not return an error because startup time is based
# number of items like UDFs installed and a long startup may not
# be an error -- it merely gives the startup a chance
# the $XCE_USRNODE_WAIT must be less that systemd
# TimeoutStartSec, or systemd will kill the service
for i in `seq 1 $XCE_USRNODE_WAIT`; do
    if ! isXcalarRunning; then
        sleep 1
        continue
    fi

    getCliBackendVersion $apiPortNum
    if [ -n "$cliBackendVersion" ]; then
        # quit if xcalar is running
        break
    fi
    sleep 1
done
exit 0
