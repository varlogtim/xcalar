#!/bin/bash

# given a XLRGUIDIR and XLRDIR, checks if there's an API mismatch and it there is
# it checks out the previous version of XCE until it finds one that matches or MAX_NUM_SHAS are checked

set -e

MAX_NUM_SHAS=10

die() {
    [ $# -gt 0 ] && echo "[ERROR] $*" >&2
    exit 1
}

debug() {
    echo "[DEBUG] $*" >&2
}

XLRDIR=${1:-"$XLRDIR"}
XLRGUIDIR=${2:-"$XLRGUIDIR"}

[ -n "$XLRDIR" ] && [ -n "$XLRGUIDIR" ] || die "Usage: $0 <xce_repo> <xd_repo> or export \$XLRDIR and \$XLRGUIDIR"

PKG_LANG="en"
# provides apiFiles used below for checkouts
source $XLRDIR/src/data/apiFilesPaths.env

if ! $XLRDIR/bin/checkApiMismatch.sh ; then
    echo "Version (HEAD) of XCE has a mismatch, trying next ${MAX_NUM_SHAS} shas..."
    # HEAD is already current so skipping it and only fetch MAX_NUM_SHAS worth of shas
    gitshas=$(git log --format=%H -n ${MAX_NUM_SHAS} HEAD~1 -- ${apiFiles})
    for gitsha in $gitshas; do
        if ! git checkout "$gitsha" -- ${apiFiles} ; then
            die "Something is wrong, failed to checkout $gitsha. Exiting"
        fi
        if $XLRDIR/bin/checkApiMismatch.sh ; then
            echo "Version (${gitsha}) of XCE is compatible with XD, checking the full repo out..."
            if ! git checkout "$gitsha" ; then
                die "Could not checkout matching XCE repo ${gitsha}"
            fi
            if ! git submodule update --init --recursive xcalar-idl ; then
                die "Could not checkout xcalar-idl for matching XCE repo ${gitsha}"
            fi
            echo "Version (${gitsha}) of XCE is ready to go"
            exit 0
        fi
        echo "Version (${gitsha}) of XCE is not compatible, trying next sha..."
    done
    die "$MAX_NUM_SHAS checkouts were analyzed and no matching repo was found"
fi

echo "Current version of XCE is already compatible. Exiting..."
exit 0
