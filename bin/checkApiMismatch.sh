#!/bin/bash

# Exits 1 if the current XLRDIR and XLRGUIDIR have a thrift or xcrpc mismatch

set -e

die() {
    [ $# -gt 0 ] && echo "[ERROR] $*" >&2
    exit 1
}

debug() {
    echo "[DEBUG] $*" >&2
}

XLRDIR=${1:-"$XLRDIR"}
XLRGUIDIR=${2:-"$XLRGUIDIR"}

[ -n "$XLRDIR" ] && [ -n "$XLRGUIDIR" ] || die "Usage: $0 <xce_repo> <xd_repo>  or export \$XLRDIR and \$XLRGUIDIR"

versionMatch=0
scriptPath=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)

PKG_LANG="en"
# provides thriftDefFileList, thriftSigFile, xcrpcDefDir, xcrpcSigFile used below
source $XLRDIR/src/data/apiFilesPaths.env

# Check Thrift Api Signature
thriftSig=$( eval "$XLRDIR/bin/genVersionSig.py -i '${thriftDefFileList[@]}'" | grep VersionSignature | cut -f2 -d'"')

if grep -q $thriftSig $thriftSigFile ; then
    debug "Thrift: Match"
else
    versionMatch=1
    debug "Thrift(backendSig=$thriftSig): Mismatch"
fi

# Check XCRPC Signature

xcrpcSig=$( $XLRDIR/bin/genProtoVersionSig.py -d $xcrpcDefDir | grep ProtoAPIVersionSignature | cut -f2 -d'"')

if grep -q $xcrpcSig $xcrpcSigFile ; then
    debug "Xcrpc: Match"
else
    versionMatch=1
    debug "Xcrpc(backendSig=$xcrpcSig): Mismatch"
fi

exit $versionMatch
