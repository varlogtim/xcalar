#!/bin/bash
#
# shellcheck disable=SC1090,SC1091
if ((XTRACE)); then
    set -x
    export PS4='# [${PWD}] ${BASH_SOURCE#$PWD/}:${LINENO}: ${FUNCNAME[0]}() - ${container:+[$container] }[${SHLVL},${BASH_SUBSHELL},$?] '
fi

. /etc/profile
if test -f ~/.bash_profile; then
    . ~/.bash_profile
elif test -f ~/.profile; then
    . ~/.profile
fi

exec "$@"
