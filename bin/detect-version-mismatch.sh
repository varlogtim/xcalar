#!/bin/bash

# Detects version mismatch by comparing thrift API signatures in source code of
# xcalar and xcalar gui builds.
# Returns 0 if no version mismatch, 1 if version mismatch.
# This does NOT require Xcalar to be running.
#  usage: bash detect-version-mismatch.sh

set -e

: "${XLRDIR:?Need to set non-empty XLRDIR}"
: "${XLRGUIDIR:?Need to set non-empty XLRGUIDIR}"

XD_VERSION_FILE="${XD_VERSION_FILE:-$XLRGUIDIR/xcalar-gui/assets/js/thrift/XcalarApiVersionSignature_types.js}"
XCE_VERSION_FILE="${XCE_VERSION_FILE:-$XLRDIR/buildOut/src/lib/libenum/XcalarApiVersionSignature.enum}"

if [ ! -f "$XD_VERSION_FILE" ]; then
    echo "Missing XD version file $XD_VERSION_FILE (have you built the xcalar-gui project?)" >&2
    exit 1
fi
if [ ! -f "$XCE_VERSION_FILE" ]; then
    echo "Missing XCE version file $XCE_VERSION_FILE (have you built xcalar?)" >&2
    exit 1
fi

# prints frontend signature (XcalarApiVersionSignature key of XcalarApiVersionT
# hash in <xcalar-gui build>/assets/js/thrift/XcalarApiVersionSignature_types.js
# in minified versions of this file (happens make installer builds),
# whitespace are removed from around ':' char, and enclosing single quotes
# removed from XcalarApiVersionSignature key
get_frontend_signature() {
    grep -oP "XcalarApiVersionSignature(')?\s*:\s*\d+" "$1" | grep -oP ":\s*\d+" | grep -oP "\d+"
}

# prints backend signature (XcalarApiVersionSignature:"<long sha>" key of XcalarApiVersion
# enum in <xcalar build>/src/lib/libenum/XcalarApiVersionSignature.enum
get_backend_signature() {
    grep -oP 'XcalarApiVersionSignature:"[a-zA-Z0-p]+" = \d+' "$1" | grep -oP "= \d+" | grep -oP "\d+"
}

frontend_signature=$(get_frontend_signature "$XD_VERSION_FILE")
backend_signature=$(get_backend_signature "$XCE_VERSION_FILE")

if [ "$frontend_signature" == "$backend_signature" ]; then
    exit 0
else
    exit 1
fi
