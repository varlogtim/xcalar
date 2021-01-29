#!/bin/bash
#
# shellcheck disable=SC2155,SC1091

if ((XTRACE)); then
    set -x
    export PS4='# [${PWD}] ${BASH_SOURCE#$PWD/}:${LINENO}: ${FUNCNAME[0]}() - ${container:+[$container] }[${SHLVL},${BASH_SUBSHELL},$?] '
fi

set -e

unset CDPATH

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

setup_xcalarenv() {
    export XLRDIR="$(cd "$DIR"/.. && pwd)"
    cd "$XLRDIR"
    . doc/env/xc_aliases
    if ! test -e "$XLRGUIDIR"; then
        if test -e "$XLRDIR/xcalar-gui"; then
            export XLRGUIDIR=$XLRDIR/xcalar-gui
        elif test -e "$XLRDIR/../xcalar-gui"; then
            export XLRGUIDIR="$(cd "$XLRDIR/../xcalar-gui" && pwd)"
        fi
    fi

    if ! test -e "$XLRINFRADIR"; then
        if test -e "$XLRDIR/xcalar-infra"; then
            export XLRINFRADIR=$XLRDIR/xcalar-infra
        elif test -e "$XLRDIR"/../xcalar-infra; then
            export XLRINFRADIR="$(cd "$XLRDIR"/../xcalar-infra && pwd)"
        fi
    fi

    PATH="/opt/xcalar/bin:$PATH"
    if test -e "$XLRINFRADIR"/bin/infra-sh-lib; then
        PATH="$XLRINFRADIR/bin:$PATH"
    else
        echo "WARNING: XLRINFRADIR not autodetected and not set" >&2
    fi
    export PATH="$XLRDIR/bin:$PATH"
}

build_container() {
    local MFLAGS='-s V=0'
    if [ -n "$BUILD_ID" ]; then
        MFLAGS='V=1'
    fi

    if ! (
        cd "$XLRDIR"/docker && \
        make ${MFLAGS}
    ); then
        echo >&2 "Docker build failed"
        exit 1
    fi
}

setup_xcalarenv
build_container
crun el7-build /bin/bash -x "$DIR"/with_profile.sh "$@"
