#!/bin/bash
DIR="$(cd "$(dirname ${BASH_SOURCE[0]})" && pwd)"

INSTALL_DIR="$(dirname $(dirname $(dirname $DIR) ) )"

# Change defaults for your installation in the following file
if [ -r "/etc/default/xcalar" ]; then
    . /etc/default/xcalar
elif [ -r "$INSTALL_DIR/opt/xcalar/etc/default/xcalar" ]; then
    . "$INSTALL_DIR/opt/xcalar/etc/default/xcalar"
    if [ -r "$INSTALL_DIR/etc/default/xcalar" ]; then
        . "$INSTALL_DIR/etc/default/xcalar"
    fi
elif [ -r "$INSTALL_DIR/etc/default/xcalar" ]; then
    . "$INSTALL_DIR/etc/default/xcalar"
fi

XCE_CONFIG="${XCE_CONFIG:-/etc/xcalar/default.cfg}"
XLRDIR="${XLRDIR:-/opt/xcalar}"
LIBHDFS3_CONF="${LIBHDFS3_CONF:-/etc/xcalar/hdfs-client.xml}"
PATH="$XLRDIR/bin:$PATH"
XCE_WORKDIR="${XCE_WORKDIR:-/var/tmp/xcalar-root}"
XCE_ASUP_TMPDIR="${XCE_ASUP_TMPDIR:-/var/tmp}"

export XCE_CONFIG XCE_LOGDIR XLRDIR LIBHDFS3_CONF PATH XCE_WORKDIR XCE_ASUP_TMPDIR

. determineNodeId.sh

# Kerberos5 config specified in XCE_CONFIG
KRB5_TRACE="${KRB5_TRACE:-$XCE_LOGDIR/krb5-trace.log}"
test -n "$KRB5_CONFIG" && export KRB5_CONFIG
test -n "$KRB5_KDC_PROFILE" && export KRB5_KDC_PROFILE
test -n "$KRB5_KTNAME" && export KRB5_KTNAME
test -n "$KRB5CCNAME" && export KRB5CCNAME
test -n "$KPROP_PORT" && export KPROP_PORT
test -n "$KRB5_TRACE" && export KRB5_TRACE
test -n "$KRB5RCACHETYPE" && export KRB5RCACHETYPE
test -n "$KRB5RCACHEDIR" && export KRB5RCACHEDIR

xcalarRootPath="$(awk -F'=' '/^Constants.XcalarRootCompletePath=/{print $2}' $XCE_CONFIG)"
XCE_LOGDIR="$(awk -F'=' '/^Constants.XcalarLogCompletePath=/{print $2}' $XCE_CONFIG)"
XCE_LOGDIR="${XCE_LOGDIR:-/var/log/xcalar}"

if test -e "$XLRDIR/.git"; then
    xcalarVersion="$(git describe)"
elif test -e "$INSTALL_DIR/etc/xcalar/VERSION"; then
    read xcalarVersion < "$INSTALL_DIR/etc/xcalar/VERSION"
elif test -e /etc/redhat-release; then
    xcalarVersion="$(rpm -q xcalar)"
elif command -v dpkg; then
    dpkgVersion="$(dpkg -s xcalar | grep '^Version:' | cut -d' ' -f2)"
    if [ "$dpkgVersion" != "" ]; then
        xcalarVersion="xcalar-${dpkgVersion}"
    fi
fi
if test -z "$xcalarVersion"; then
    xcalarVersion="$(cd $XLRDIR/share/doc/ && ls -d *)"
fi
if test -z "$xcalarVersion"; then
    xcalarVersion="xcalar-1.0.unknown"
fi

determineNodeId

if [ -z "$nodeId" ]; then
    echo >&2 "Could not determine node ID for this host. Please check your config file (${XCE_CONFIG})."
    exit 1
fi
nodeIdArr=($nodeId)

xcalarVersion="${xcalarVersion##xcalar-}"
UUID=${UUID:-$(< /proc/sys/kernel/random/uuid)}
# Default to 0 when support case id is not provided. This happens for internal runs and nightly runs.
CASE_ID=${1:-0}
python3.6 $DIR/Support.py "$UUID" "${nodeIdArr[0]}" "$XCE_CONFIG" "$xcalarRootPath" "$XCE_LOGDIR" "$xcalarVersion" "false" "$CASE_ID" "$nodeId"
