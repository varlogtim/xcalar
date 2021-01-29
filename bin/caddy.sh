#!/bin/bash
#
# shellcheck disable=SC2086

caddyDefaults() {
    XCE_HTTPS_PORT=${XCE_HTTPS_PORT:-8443}
    CADDYHOME=${CADDYHOME:-/tmp/caddy-$(id -u)/$XCE_HTTPS_PORT}
    CADDYPIDFILE=${CADDYPIDFILE:-$CADDYHOME/caddy.pid}
    CADDYFILE=${CADDYFILE:-$XLRDIR/conf/Caddyfile-dev}
    CADDYROOT=${CADDYROOT:-$XLRGUIDIR/xcalar-gui}
    if [ -w "$XCE_LOGDIR" ]; then
        if [ "$XCE_HTTPS_PORT" = 8443 ]; then
            CADDYLOG="${XCE_LOGDIR}/caddy.log"
            CADDYERR="${XCE_LOGDIR}/caddy-errors.log"
        else
            # Append the port if it's a non standard/default port since
            # XCE_LOGDIR is shared between multiple instances of xcalar
            CADDYLOG="${XCE_LOGDIR}/caddy-${XCE_HTTPS_PORT}.log"
            CADDYERR="${XCE_LOGDIR}/caddy-${XCE_HTTPS_PORT}-errors.log"
        fi
    else
        CADDYLOG="$CADDYHOME/caddy.log"
        CADDYERR="$CADDYHOME/caddy-errors.log"
    fi
}

pidAlive() {
    kill -0 $1 2>/dev/null
}

log() {
    local now
    now="$(date +'%FT%T %z')"
    echo "[$now] $*" | tee -a $CADDYERR >&2
}

# shellcheck disable=SC2181
startCaddy() {
    mkdir -p $CADDYHOME \
      && cd $CADDYHOME \
      || return 1
    truncate -s 0 $CADDYLOG $CADDYERR 2>/dev/null || true
    log "Starting caddy in $PWD on port $XCE_HTTPS_PORT"
    exec caddy -pidfile $CADDYPIDFILE \
              -port $XCE_HTTPS_PORT \
              -https-port $XCE_HTTPS_PORT \
              -root $CADDYROOT \
              -conf $CADDYFILE >>$CADDYLOG 2>>$CADDYERR </dev/null &
    if [ $? -ne 0 ]; then
        cat >&2 $CADDYERR
        echo >&2 "ERROR: Failed to start caddy in $PWD on port $XCE_HTTPS_PORT"
        return 1
    fi
    CADDYPID=$!
    until test -f $CADDYPIDFILE && pidAlive $(< $CADDYPIDFILE); do
        if ! pidAlive $CADDYPID; then
            cat >&2 $CADDYERR
            echo >&2 "ERROR: Failed to start caddy in $PWD"
            return 1
        fi
        sleep 0.5
    done
    (
    echo "CADDYPIDFILE=$CADDYPIDFILE"
    echo "CADDYHOME=$CADDYHOME"
    echo "CADDYFILE=$CADDYFILE"
    echo "CADDYROOT=$CADDYROOT"
    echo "CADDYLOG=$CADDYLOG"
    echo "CADDYERR=$CADDYERR"
    echo "CADDYURL=https://localhost:${XCE_HTTPS_PORT}"
    echo "XCE_HTTPS_PORT=$XCE_HTTPS_PORT"
    ) | tee $CADDYHOME/caddy.env
}

stopCaddy() {
    if ! [ -e $CADDYPIDFILE ]; then
        return 0
    fi
    RETRY=${1:-5}
    CADDYPID=$(cat $CADDYPIDFILE)
    log "Stopping caddy in $CADDYHOME on port $XCE_HTTPS_PORT (PID: $CADDYPID)"
    #logging additional information to troubleshoot ENG-8652
    PIDINFO=`ps -o pid,ppid,lstart,command -p $$`
    PPIDINFO=`ps -o pid,command -p $PPID`
    log "$PIDINFO"
    log "$PPIDINFO"
    pkill -F $CADDYPIDFILE || true
    sleep 1
    if CADDYPID=$(cat $CADDYPIDFILE 2>/dev/null); then
        for((try=0; try < RETRY; try++)); do
            if ! pidAlive $CADDYPID; then
                rm -f $CADDYPIDFILE
                return 0
            fi
            if [ $try -gt 2 ]; then
                kill -KILL $CADDYPID 2>/dev/null
            else
                kill -TERM $CADDYPID 2>/dev/null
            fi
            sleep 2
        done
        return 1
    fi
    return 0
}

# Checks whether the currently running caddy is using the same parameters
# as the ones being specified for this run. When calling start twice, we
# don't bother to stop/start caddy, but we must do so if any of the settings
# have changed.
consistentCaddy() {
    if ! grep -q '^CADDYROOT='$CADDYROOT'$' $CADDYHOME/caddy.env ||
        ! grep -q '^CADDYFILE='$CADDYFILE'$' $CADDYHOME/caddy.env; then
            echo >&2 "WARNING: Stale caddy run in $CADDYHOME"
            return 1
    fi
    return 0
}

statusCaddy() {
    if ! CADDYPID=$(cat $CADDYPIDFILE 2>/dev/null); then
        return 1
    fi
    if ! pidAlive $CADDYPID; then
        return 1
    fi
    local code
    if code=$(curl -k -s https://localhost:${XCE_HTTPS_PORT} -o /dev/null -w '%{http_code}'); then
        if [[ $code =~ ^[23]0 ]]; then
            return 0
        fi
        echo >&2 "WARNING: https://localhost:${XCE_HTTPS_PORT} is up, but responded with HTTP $code"
        if ! test -e "$CADDYROOT"; then
            echo >&2 "WARNING: Your root $CADDYROOT doesn't exist"
        fi
        return 0
    fi
    return 1
}

usage() {
    cat <<EOF
    usage: $0 (start|stop|status) [-p port] [-d homedir] [-r www-root] [-c caddyfile]

    Will run an instance of caddy with the given settings and print a set of env vars
    that can be used to interact with it.
EOF
}

main()  {
    ACTION=start
    while [ $# -gt 0 ]; do
        cmd="$1"
        shift
        case "$cmd" in
            start|stop|status) ACTION="$cmd";;
            -p|--port) XCE_HTTPS_PORT="$1"; shift;;
            -d|--homedir) CADDYHOME="$1"; shift;;
            -r|--root) CADDYROOT="$1"; shift;;
            -c|--conf) CADDYFILE="$1"; shift;;
            -h|--help) usage; exit 0;;
            *) echo >&2 "ERROR: Unknown argument: $cmd"; exit 1;;
        esac
    done
    caddyDefaults
    case "$ACTION" in
        start)
            if statusCaddy >/dev/null 2>&1; then
                if ! consistentCaddy; then
                    stopCaddy 5
                    startCaddy
                else
                    CADDYPID=$(< $CADDYPIDFILE)
                    echo >&2 "Caddy is already running on port $XCE_HTTPS_PORT, PID: $CADDYPID"
                    cat $CADDYHOME/caddy.env
                fi
            elif startCaddy; then
                CADDYPID=$(< $CADDYPIDFILE)
                echo >&2 "Started caddy on port $XCE_HTTPS_PORT, PID: $CADDYPID"
            else
                echo >&2 "ERROR: Failed to start caddy on port $XCE_HTTPS_PORT"
                return 1
            fi
            ;;
        stop)
            if ! statusCaddy >/dev/null 2>&1; then
                echo >&2 "Caddy is not running on port $XCE_HTTPS_PORT"
            elif stopCaddy 5; then
                echo >&2 "Stopped caddy on port $XCE_HTTPS_PORT"
            else
                echo >&2 "ERROR: Failed to stop caddy on port $XCE_HTTPS_PORT, PID: $CADDYPID"
                return 1
            fi
            ;;
        status)
            if statusCaddy; then
                echo >&2 "Caddy is running on port $XCE_HTTPS_PORT, with PID: $CADDYPID"
                cat $CADDYHOME/caddy.env
            else
                echo >&2 "Caddy is not running"
                return 1
            fi
            ;;
        *) echo >&2 "ERROR: Unknown action: $ACTION"; return 1;;
    esac
}

main "$@"
