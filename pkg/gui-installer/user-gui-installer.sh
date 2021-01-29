#!/bin/bash

# This should be passed from
test -n "$ROOTDIR" || ROOTDIR="$(cd "$(dirname ${BASH_SOURCE[0]})" && pwd)"
test -n "$MKSHARPWD" && cd "$MKSHARPWD"
test -e "$ROOTDIR/etc/default/xcalar" && . "$ROOTDIR/etc/default/xcalar"

OLD_TMPDIR="${TMPDIR:-/tmp}"

say () {
    echo >&2 "$*"
}

if ! [ -w "$OLD_TMPDIR" ]; then
    say "No write permission on $OLD_TMPDIR"
    say "Exiting..."
    exit 1
fi

port_in_use () {

    if $ROOTDIR/opt/xcalar/bin/ncat $DEPLOY_HOST $1 </dev/null >/dev/null 2>&1; then
        say "$2"
        PORT_IN_USE=1
    fi
}

. $ROOTDIR/opt/xcalar/bin/osid > /dev/null
VERSTRING="$(_osid --full)"

case ${OSID_NAME} in
    el|rhel)
        distro="el${OSID_VERSION}"
        ;;
    *)
        distro="${OSID_NAME}${OSID_VERSION}"
        ;;
esac

DEPLOY_HOST="$(hostname -f)"

if [ "$DEPLOY_HOST" = localhost ]; then
    DEPLOY_IP="127.0.0.1"
else
    ns1="$(grep '^nameserver' /etc/resolv.conf | head -1 | awk '{print $2}')"
    ns2="8.8.8.8"
    ethdev="$(ip route get $ns2 | head -1 | awk '{print $3}')"
    DEPLOY_IP="$(ip route get $ns2 | head -1 | awk '{print $(NF)}')"
fi

NO_PING=0

while [ $# -gt 0 ]; do
    cmd="$1"
    case "$cmd" in
        -n|--no_ping) NO_PING=1;;
        -r|--cluster_redirect)
            if [ -z "$2" ]; then say "No cluster host/ip specified"; exit 1; fi
            XCE_CLUSTER_HOST="$2"
            export XCE_CLUSTER_HOST
            shift
            ;;
        *) break;;
    esac
    shift
done

export NO_PING

export XCE_HTTPS_PORT=8543
export XCE_EXP_PORT=12224

export XCE_INSTALLER_ROOT=$ROOTDIR/opt/xcalar
export NODE_PATH="$ROOTDIR/opt/xcalar/xcalar-gui/services/expServer:$ROOTDIR/opt/xcalar/xcalar-gui/services/expServer/node_modules"

if ! [ -e $ROOTDIR/opt/xcalar/.started ]; then
    tar xzf $ROOTDIR/gui-installer.tar.gz -C $ROOTDIR
    tar xzf $ROOTDIR/node-modules.tar.gz -C ${ROOTDIR}/opt/xcalar/xcalar-gui/services/expServer
    tar xzf $ROOTDIR/sshpass.tar.gz -C $ROOTDIR
    tar xzf $ROOTDIR/netcat.tar.gz -C $ROOTDIR

    PORT_IN_USE=0
    port_in_use $XCE_HTTPS_PORT "Caddy port ${XCE_HTTPS_PORT} is in use"
    port_in_use $XCE_EXP_PORT "NodeJS port ${XCE_EXP_PORT} is in use"
    if [ "$PORT_IN_USE" == "1" ]; then
        say "Exiting..."
        exit 1
    fi

    export TMPDIR=${XCE_INSTALLER_ROOT}/tmp
    export PATH="${ROOTDIR}/opt/xcalar/bin:$PATH"

    $ROOTDIR/opt/xcalar/bin/caddy -quiet -conf $ROOTDIR/opt/xcalar/etc/caddy/Caddyfile -root $ROOTDIR/opt/xcalar/xcalar-gui |& tee -a ${TMPDIR}/xcalar-install-caddy.log ${TMPDIR}/xcalar-install-combined.log &
    CADDY_PID=$(jobs -p | tail -1)
    $ROOTDIR/opt/xcalar/bin/npm start --prefix $ROOTDIR/opt/xcalar/xcalar-gui/services/expServer |& tee -a ${TMPDIR}/xcalar-install-expserver.log ${TMPDIR}/xcalar-install-combined.log &
    NODE_PID=$(jobs -p | tail -1)
    # the wait below ensures that caddy must start
    # this verifies that the expServer started and exits if it does not
    # after one minute.
    for i in `seq 1 4`; do
        sleep 15
        kill -0 "$NODE_PID"
        NODE_STARTED=$?
        [ "$NODE_STARTED" == "0" ] && break
    done
    if [ "$NODE_STARTED" != "0" ]; then
        say "Installer failure: the expServer did not start.  Exiting..."
        kill -9 "$CADDY_PID"
        exit 1
    fi

    touch $ROOTDIR/opt/xcalar/.started
    trap "kill -9 $CADDY_PID $NODE_PID" INT

    say ""
    say "Running installation shell archive, press Ctrl-C to stop"
    say ""
    say "Please open your browser to one of:"
    say ""
    say "    https://${DEPLOY_IP}:${XCE_HTTPS_PORT}"
    say "    https://${DEPLOY_HOST}:${XCE_HTTPS_PORT}"
    say ""
    say ""
    say "Or use the appropriate IP for where you ran this script from."

    echo "Type Ctrl-C to stop installer"
    wait $CADDY_PID

    TSTAMP=$(date '+%s')
    if [ "$(ls "${TMPDIR}" | wc -l)" -gt 0 ]; then
        installer_log_name="${OLD_TMPDIR}/gui-install-logs.${TSTAMP}.$$.tgz"
        tar czf $installer_log_name -C ${TMPDIR} .
        echo "Installer logs can be found at $installer_log_name"
    fi
fi

