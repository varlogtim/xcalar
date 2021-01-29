#!/bin/bash

set -e

FEXT="proto"
ALLSHAS=""

NL=$'\n'

if [ "$#" -ne 1 ]
then
    echo "Missing path argument"
    echo "  $0 <proto path>"
    exit 1
fi

PBPATH="$1"
# Local builds need GIT_DIR to be set
if [ -z "$GIT_DIR" ]
then
    export GIT_DIR="$XLRDIR/.git"
fi

genInt2Sha() {
    FLIST="$(find $1 -name "*.$FEXT")"

    for f in ${FLIST}
    do
        f="$(echo $f | sed 's/.*xcalar-idl\///')"
        if ! VERS=$(set -o pipefail; cd $XLRDIR/xcalar-idl && GIT_DIR=$GIT_DIR/modules/xcalar-idl git log --reverse --pretty=format:%H -- $f | sed 's/^\|$/"/g'|tr '\n' ',')
        then
            echo >&2 "ERROR: Failed to get history for $f"
            exit 1
        fi
        if [ ! "$VERS" ];
        then
            echo >&2 "ERROR: Failed to get history for $f, VERS is empty"
            echo >&2 "XLRDIR is $XLRDIR"
            echo >&2 "GIT_DIR is $GIT_DIR"
            exit 1
        fi
        ALLSHAS="${VERS},${ALLSHAS}"
        idlName="$(basename $f .$FEXT)"
        varName="pbIntToSha_${idlName}"

        cat <<EOF

// $idlName: integer version numbers to SHAs mapping
// TODO: Add reverse mapping...
static const char *${varName}[] = {$VERS};

// $idlName: return a string literal of the current version's git SHA
static inline int64_t pbGetNumShas_${idlName}(void) {
    const int64_t numShas = sizeof(${varName}) / sizeof(*${varName});
    assert(numShas > 0);
    return (numShas);
}

// $idlName: return a string literal of the current version's git SHA
static inline const char *pbGetCurrSha_${idlName}(void) {
    const int64_t idx = pbGetNumShas_${idlName}() - 1;

    assert(idx >= 0);
    return (${varName}[idx]);
}

// $idlName: return a string literal of the first version's git SHA
static inline const char *pbGetFirstSha_${idlName}(void) {
    assert(sizeof(${varName}) > 0);
    return (${varName}[0]);
}
EOF
    done
}

isDirtyWorkspace() {
    if [ -n "$BUILD_URL" ] || git diff --quiet HEAD
    then
        echo "static bool constexpr isDirtyWorkspace = false;"
    else
        echo "static bool constexpr isDirtyWorkspace = true;"
    fi
}

hasLocalCommits() {
    if [ -n "$BUILD_URL" ] || git diff --quiet gerrit/trunk..HEAD
    then
        echo "static bool constexpr hasLocalCommits = false;"
    else
        echo "static bool constexpr hasLocalCommits = true;"
    fi
}


cat <<EOF
// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DURABLEVERSIONS_H
#define _DURABLEVERSIONS_H

#include "DurableObject.pb.h"
#include "assert.h"

namespace DurableVersions {

$(isDirtyWorkspace)
$(hasLocalCommits)

// Map a major version number enum to a version string representing the
// version's name in the IDL
static inline const char *pbVerMap(const int verNum) {
    // TODO: Automatically generate enough of these depending on how many
    // IDLs we find.
    // "v0" is invalid version in IDL
    static const char *pbVerMap[] = {"v0", "v1", "v2", "v3", "v4", "v5"};
    const int numElms = sizeof(pbVerMap) / sizeof(*pbVerMap);
    if (verNum < numElms) {
        return (pbVerMap[verNum]);
    } else {
        return (pbVerMap[0]);
    }
}

EOF

genInt2Sha $PBPATH

echo

cat <<EOF
// List of SHAs for all IDLs to allow us to easily check if the deserialized
// IDL is known (avoids a large unneccessary switch in C to pick out a SHA list)
static const char *pbIntToSha_AllShas[] = {${ALLSHAS}};
static inline int64_t pbGetNumShas_AllShas(void) {
    const int64_t numShas = sizeof(pbIntToSha_AllShas) / sizeof(*pbIntToSha_AllShas);
    assert(numShas > 0);
    return (numShas);
}
EOF

echo "} // namespace DurableVersions"
echo
echo "#endif // _DURABLEVERSIONS_H"
