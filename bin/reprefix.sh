#!/bin/bash
#
# Changes the shebang '#!' line in installed scripts, so that
# they can be relocated. This is primarily a problem with installed
# python scripts that have hardcoded paths to the python executable.
#
# Example usage:
#
#   $ reprefix.sh -i xcalar-python36.tar.gz -o output.tar.gz --prefix /opt/xcalar --newprefix /opt/xcalar/opt/xcalar
#   $ reprefix.sh -i /tmp/rootfs/opt/xcalar/bin --prefix /tmp/rootfs/opt/xcalar --newprefix /opt/xcalar
#
set -eu

DEFPREFIX=/opt/xcalar
DEFNEWPREFIX=/opt/xcalar/opt/xcalar
DEFPREFIX_PATH=/opt/xcalar/bin
DEFNEWPREFIX_PATH=/opt/xcalar/opt/xcalar/bin
TAPIDX=1
TAPERR=0


die() {
    echo >&2 "$1"
    exit 1
}

tap_new() {
    TAPCOUNT=${1?Must specify number of tests}
    TAPDIE=${2:-die_on_error}
    TAPIDX=1
    TAPERR=0
    echo "TAP version 13"
    echo "1..$TAPCOUNT"
}

tap_ok() {
    echo "ok $TAPIDX - $1"
    TAPIDX=$((TAPIDX+1))
}

tap_not_ok() {
    echo "not ok $TAPIDX - $1"
    TAPIDX=$((TAPIDX+1))
    TAPERR=$((TAPERR+1))
    if [ "$TAPDIE" = die_on_error ]; then
        die "ERROR: Test failure"
    fi
}

tap_ok_or_not() {
    if [ $1 -eq 0 ]; then
        tap_ok "$2"
    else
        tap_not_ok "$2"
    fi
}

reprefix_usage() {
    cat << EOF
    usage: $0 --input input.tar OR --directory DIR [--output output.tar.gz] [--prefix PREFIX] [--newprefix NEWPREFIX]
                             [--prefix-path PREFIX_PATH] [--newprefix-path NEWPREFIX_PATH]

    -i|--input        input filename tar
    -d|--directory    files are in this use this directory

    -o|--output       output tar for input (def: input.tar.gz is replaced when specified)
    --prefix          current prefix  (def: $DEFPREFIX)
    --newprefix       new prefix path (def: $DEFNEWPREFIX)
    --prefix-path     current prefix or string to search for (def: $DEFPREFIX_PATH)
    --newprefix-path  new prefix path to use (def: $DEFNEWPREFIX_PATH)

    Example usage:

    \$ reprefix.sh -i xcalar-python36.tar.gz -o output.tar.gz --prefix /opt/xcalar --newprefix /opt/xcalar/opt/xcalar
    \$ reprefix.sh -i /tmp/rootfs/opt/xcalar/bin --prefix /tmp/rootfs/opt/xcalar --newprefix /opt/xcalar

EOF
}

reprefix() {
    local pkgtmp=''
    local pkg=''
    local pigz="$(command -v pigz || true)"
    local pigzcmd="${pigz:+--use-compress-program=$pigz}"
    local changed=0
    local output=''
    local tarcmd='--owner root --group root'

    local PREFIX=${PREFIX:-/opt/xcalar}
    local NEWPREFIX=${NEWPREFIX:-}
    local PREFIX_PATH=${PREFIX_PATH:-}
    local NEWPREFIX_PATH=${NEWPREFIX_PATH:-}

    if [ $# -eq 0 ]; then
        reprefix_usage
        exit 0
    fi

    while [ $# -gt 0 ]; do
        cmd="$1"
        shift
        case "$cmd" in
            -i | --input)
                pkg="$1"
                shift
                ;;
            -d | --directory)
                pkg="$1"
                shift
                ;;
            -o | --output)
                output="$1"
                shift
                ;;
            --prefix)
                PREFIX="${1%/bin}"
                shift
                ;;
            --newprefix)
                NEWPREFIX="${1%/bin}"
                shift
                ;;
            --prefix-path)
                PREFIX_PATH="$1"
                shift
                ;;
            --newprefix-path)
                NEWPREFIX_PATH="$1"
                shift
                ;;
            --)
                pkg="$1"
                shift
                break
                ;;
            -*)
                die "ERROR: Unknown parameter: $cmd"
                ;;
            *)
                die "ERROR: Please use -i $cmd or -d $cmd"
                ;;
        esac
    done
    if [ -z "$pkg" ]; then
        die "Must specify package or directory for input"
    fi

    if [ -z "$NEWPREFIX" ]; then
        if test -f "$pkg"; then
            NEWPREFIX="$PREFIX$PREFIX"
        elif test -d "$pkg/$PREFIX/bin"; then
            NEWPREFIX=$pkg/$PREFIX/bin
        elif test -d "$pkg/bin"; then
            NEWPREFIX=$pkg/bin
        elif test -d "$pkg"; then
            NEWPREFIX="$pkg"
        else
            die "Don't know how to handle $pkg"
        fi
    fi
    if [ -z "$PREFIX_PATH" ]; then
        PREFIX_PATH=$PREFIX/bin
    fi
    if [ -z "$NEWPREFIX_PATH" ]; then
        NEWPREFIX_PATH=${NEWPREFIX}/bin
    fi
    if test -f $pkg && [[ $pkg =~ \.(tar.gz|tar|tgz)$ ]]; then
        if ! pkgtmp=$(mktemp -d -t pkgtmp-XXXXXX); then
            die "ERROR: Failed to create tempdir"
        fi
        if ! tar axf ${pkg} -C $pkgtmp; then
            echo >&2 "ERROR: Couldn't extract ${pkg} in $pkgtmp"
            exit 1
        fi
        if ! pushd ${pkgtmp} > /dev/null; then
            die "ERROR: Couldn't cd to ${pkgtmp}"
        fi
    elif test -d $pkg; then
        if ! pushd ${pkg}; then
            die "ERROR: Couldn't cd to ${pkg}/${PREFIX}/bin or to $pkg"
        fi
    else
        die "ERROR: Don't understand how to work with $pkg"
    fi

    FILES=($(find . -type f -executable))
    for ii in "${FILES[@]}"; do
        if [[ "$(basename $ii)" == python3.6m-config ]]; then
            sed -i '1 s,^#!/opt/xcalar/.*$,#!/bin/bash,' "$ii"
            continue
        fi
        if test -f "$ii" && head -1 "$ii" | grep -q '^#!'${PREFIX_PATH}''; then
            if sed -i '1 s,^#!'${PREFIX_PATH}',#!'${NEWPREFIX_PATH}',' "$ii"; then
                changed=$((changed + 1))
            else
                warnings=$((warnings + 1))
                echo >&2 "WARNING: Reprefixing $ii to $NEWPREFIX failed"
            fi
        fi
    done
    popd > /dev/null
    if test -d "$pkgtmp"; then
        tar caf ${pkg}.$$ ${pigzcmd} ${tarcmd} -C ${pkgtmp} .
        mv -f ${pkg}.$$ ${output:-$pkg}
        echo "Wrote ${output:-$pkg}"
        rm -rf ${pkgtmp}
    elif [ -n "$output" ]; then
        tar caf "$output" ${pigzcmd} ${tarcmd} -C "$pkg" .
        echo "Wrote $output"
        echo "Modified $pkg"
    else
        echo "Modified $pkg"
    fi
}

test_reprefix_usage() {
    cat <<EOF
    usage: $0 [URL]

    optional positional arguments:
        URL     URL to xcalar-python.tar.gz to use as a test base

EOF
}

test_reprefix() {
    local URL=${1:-http://netstore/infra/packages/xcalar-python36-3.6.4-110.tar.gz}
    local IN=xcalar-python36.tar.gz
    local OUT=output.tar.gz
    curl -sSfL "$URL" -o $IN

    set +e
    tap_new 4 die_on_error
    (reprefix -i $IN -o $OUT --prefix /opt/xcalar --newprefix /opt/xcalar/opt/xcalar)
    tap_ok_or_not $? reprefix
    tar zxf $OUT -O ./opt/xcalar/bin/supervisord | head -1 | grep -q '^#!/opt/xcalar/opt/xcalar/bin/python3.6$'
    tap_ok_or_not $? "newprefix in supervisord"
    tar zxf $OUT -O ./opt/xcalar/bin/supervisorctl | head -1 | grep -q '^#!/opt/xcalar/opt/xcalar/bin/python3.6$'
    tap_ok_or_not $? "bad prefix in original supervisord"
    tar zxf $OUT -O ./opt/xcalar/bin/python3.6m-config | head -1 | grep -q '^#!/bin/bash$'
    tap_ok_or_not $? "/bin/bash for python3.6m-config"
    rm -f $IN $OUT
}

PROG=$(basename "$0" .sh)
PROG="${PROG//-/_}"
if [ $# -gt 0 ] && [[ $1 =~ (-h|--help) ]]; then
    eval ${PROG}_usage
    exit 0
fi

case "$PROG" in
    reprefix) reprefix "$@" ;;
    test_reprefix) test_reprefix ;;
    *) die "$0 should be named reprefix.sh or test_reprefix.sh" ;;
esac
