#!/bin/bash
#
# $ modify-installer.sh <xcalar-1.x.y.z_SHA1-installer>
#
# Produces 4 new installers suffixed with -el6, el7, -deb, and -all
# containing  platform specific installers, or -all containing all
# platforms.
#
# usage:
#  modify-installer.sh xcalar-1.0-800-installer <optional: script.sh>
#
# produces:
#  xcalar-1.0-800-installer-all (equivalent to node installer, all platforms)
#  xcalar-1.0-800-installer-el6 (only el6 bits)
#  xcalar-1.0-800-installer-el7 (only el7 bits)
#  xcalar-1.0-800-installer-deb (only debian bits)
#
set -e
DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
DEFAULT_MODIFY_SCRIPT="$DIR/default-modify-installer.sh"
XCALAR_GPG=$XLRDIR/src/data/RPM-GPG-KEY-Xcalar
IUS_GPG=$XLRDIR/src/data/IUS-COMMUNITY-GPG-KEY
XCALAR_SIG="faf9342363a342a9"
IUS_SIG='da221cdf9cd4953f'


die () {
    local rc=$1
    shift
    syslog "ERROR($rc): $*"
    exit $rc
}


say () {
    echo >&2 "$*"
}

usage () {
    cat >&2 <<XEOF

usage:
    $0 -i xcalar-1.0-800-installer [-s <script (default $DEFAULT_MODIFY_SCRIPT)>] [-p <platforms (default 'all')>] [-o <outputdir (default PWD: $PWD)>]

produces:
    xcalar-1.0-800-installer-all (equivalent to node installer, all platforms)
    xcalar-1.0-800-installer-el6 (only el6 bits)
    xcalar-1.0-800-installer-el7 (only el7 bits)
    xcalar-1.0-800-installer-deb (only debian bits)
XEOF
    exit 1
}

rpmsig () {
    rpm -qp "$1" --qf '%{NAME}-%{VERSION}-%{RELEASE} %{SIGPGP:pgpsig} %{SIGGPG:pgpsig}\n'
}

main () {
    test $# -eq 0 && set -- -h

    PLATFORMS="all"
    MODIFY_SCRIPT="$DEFAULT_MODIFY_SCRIPT"
    OUTDIR="$(pwd)"
    while getopts "hi:o:s:p:" opt "$@"; do
        case "$opt" in
            i) INSTALLER="$OPTARG";;
            o) OUTDIR="$(readlink -f $OPTARG)";;
            s) MODIFY_SCRIPT="$OPTARG";;
            p) PLATFORMS="$OPTARG";;
            h) usage;;
            \?) say "Invalid option -$OPTARG"; exit 1;;
            :) say "Option -$OPTARG requires an argument."; exit 1;;
        esac
    done

    shift $(( $OPTIND - 1 ))
    if [ -z "$INSTALLER" ] && [ $# -ge 1 ]; then
        INSTALLER="$1"
        shift
    fi

    INSTALLER_NAME="$(basename $(readlink -f $INSTALLER))"
    if [ $? -ne 0 ] || [ "$INSTALLER_NAME" = "" ]; then
        die 2 "Unable to read $INSTALLER"
    fi

    JOB_NAME="${JOB_NAME:-local}"
    TMPDIR="${TMPDIR:-/tmp/`id -u`}/${JOB_NAME}/modify-installers/$$"
    rm -rf "$TMPDIR"
    mkdir -p "$TMPDIR/xcalar-install"
    sed -n '/^__XCALAR_TARBALL__/,$p' "$INSTALLER" | tail -n+2 | tar zxif - -C "$TMPDIR/xcalar-install"
    sed -n '1,/^__XCALAR_TARBALL__$/p' "$INSTALLER" > "$TMPDIR"/installer.sh

    WORKDIR="$(pwd)"
    #tar zxif ../xcalar-installer.tar.gz
    (
        cd "$TMPDIR/xcalar-install"
        "$MODIFY_SCRIPT"
    )
    (cat $TMPDIR/installer.sh; tar czf - -C $TMPDIR/xcalar-install ./ --transform=s,^./,,g) > "${OUTDIR}/${INSTALLER_NAME}"
    chmod 0755 "${OUTDIR}/${INSTALLER_NAME}"
    rm -rf "${TMPDIR}"
}

main "$@"
