#!/bin/bash
#
# shellcheck disable=SC2016,SC2174,SC2235,SC2086,SC2046
# shellcheck disable=SC2154

export container=${container:-docker}

if ((XTRACE)); then
    set -x
    export PS4='# [${PWD}] ${BASH_SOURCE#$PWD/}:${LINENO}: ${FUNCNAME[0]}() - ${container:+[$container] }[${SHLVL},${BASH_SUBSHELL},$?] '
    DASHX=-x
fi

if [ $(id -u) != 0 ]; then
    export CONTAINER_UID=${CONTAINER_UID:-$(id -u)}
fi
export CONTAINER_UID=${CONTAINER_UID:-1000}
export CONTAINER_USER="${CONTAINER_USER:-$(awk -F: '$3 == "'$CONTAINER_UID'" {print $1}' /etc/passwd)}"
export CONTAINER_HOME="${CONTAINER_HOME:-$(awk -F: '$3 == "'$CONTAINER_UID'" {print $6}' /etc/passwd)}"
if [ $(id -u) != 0 ]; then
    export USER=${CONTAINER_USER}
    export HOME=${CONTAINER_HOME}
    export LOGNAME=${USER}
    export USERNAME=${USER}
    if [ $# -eq 0 ] || [[ $1 =~ systemd ]] || [[ $1 =~ init ]]; then
        exec /bin/bash -l
    fi
    exec "$@"
fi

if [ $$ -eq 1 ]; then
    test -e /is_container || ln -sfn /bin/true /is_container
    for f in /docker-entrypoint.d/*.sh; do
        /bin/bash $DASHX "$f" "$@"
    done
fi

if [ "$1" = fixups ]; then
    shift
    if [ $# -eq 0 ]; then
        exit 0
    fi
fi

if [ -n "$RUNAS" ]; then
    export USER="$CONTAINER_USER"
    export HOME="${CONTAINER_HOME:-/home/$USER}"
    export LOGNAME=$CONTAINER_USER
    export USERNAME="$CONTAINER_USER"
    exec su-exec $RUNAS "$@"
else
    export HOME=/root
    export USER=root
    export LOGNAME=root
    exec "$@"
fi
