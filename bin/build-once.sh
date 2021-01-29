#!/bin/bash
#
# SUMMARY:
# Manages all things related to creating and reusing the same compiled
# binaries and XLRDIR for subsequent tests and operations
# It also compiles XD, and depending on a BUILD_PROJECT env var,
# change BUILD_TYPE to dev/trunk and XCE always prod
#
# COMMANDS:
# - create, takes a XLRDIR and creates a split tar file in DST_DIR whcih
#   defaults to a tmpdir if not specified. More info below
# - extract, takes a http url to a part file list, download, re-assemble
#   and untar to DST_DIR.
# - web, takes a path DST_DIR and spins up caddy to serve files on it. Used
#   when testing this script and not saving to known http backed storage like netstore
# - fix, takes a DST_DIR (where a tar was unpacked) and fix the symlinks and paths

# CREATE command details
# - a split tar file with the current XCE and XD compiled and ready to roll
#   filename pattern is bo-${BUILD_TYPE}[-cov].partXX
# - creates a BUILD_ONCE file in $XLRDIR with info necessary to fix the tar
#   at destination and more
# - creats a bo-${BUILD_TYPE}[-cov].list listing all the part files for download
#

set -e

export COVERAGE="${COVERAGE:-OFF}"
export BUILD_TYPE="${BUILD_TYPE:-prod}"
export BUILD_PROJECT="${BUILD_PROJECT:-XCE}"

XD_BUILD_TYPE='trunk'
XC_VENV_NAME="xcve-bo"
CADDY_LOCAL_PORT=8080 # to be used when caddy serves local file to simulate netstore
DST_DIR=""
COVSTR=""

if [ "$COVERAGE" = "ON" ]; then
    COVSTR="-cov"
fi

TAR_FILENAME="bo-${BUILD_TYPE}${COVSTR}"

say() {
    echo >&2 "$*"
}

usage() {
    echo "$0 <DST_DIR> | -w --web | -f --fix | -c --create | -x --extract <url to list file>"
    echo "  DST_DIR  path where to [save the created tar | pick up the tar to unpack | tar unpacked to fix]"
    echo "  -w  start caddy to simulate a netstore http when fetching a tar from a local path"
    echo "  -f  fix symlinks and paths after unpacking"
    echo "  -c  create tar file and split it into parts"
    echo "  -x  download and extract tar file parts"
    echo "  -h  help"
}

start_caddy() {
    echo "Setting up caddy on port ${CADDY_LOCAL_PORT} to simulate a local netstore to fetch files at path $DST_DIR"
    caddy -port ${CADDY_LOCAL_PORT} -root $DST_DIR
}

create_tar() {
    if [ -z "$XLRDIR" ]; then
        say "Expected XLRDIR but not found in env"
        exit 1
    fi
    # xcalar-gui is not a subdir of xcalar checkout and we don't support thatt now
    if [ "${XLRGUIDIR##$XLRDIR}" != "/xcalar-gui" ]; then
        say "$XLRGUIDIR is not set or is not a subdir of $XLRDIR"
        exit 1
    fi

    # if we're building for XD tests this could be dev or trunk and XCE is always prod
    if [ $BUILD_PROJECT = "XD" ] ; then
        $XLRDIR/bin/checkoutXceApiMatch.sh
        export XD_BUILD_TYPE=$BUILD_TYPE
        export BUILD_TYPE='prod'
    fi

    xcEnvDir="${XLRDIR}/${XC_VENV_NAME}"
    rm -f "$XLRDIR/BUILD_ONCE"
    rm -rf "${xcEnvDir}"
    mkdir -p "${xcEnvDir}"

    . doc/env/xc_aliases
    if ! xcEnvEnterDir "${xcEnvDir}"; then
        say "Failed to enter BO venv"
        exit 1
    fi

    cmBuild clean
    # build type is defined by the env var and not passed here
    cmBuild config "$BUILD_TYPE" -DCOVERAGE="${COVERAGE}"
    # also making installedObjects so we have the necessary bits to generate an installer later
    cmBuild installedObjects
    # needed to run XCETest/XCEFuncTest
    cmBuild qa
   # build XD
    make -C "${XLRGUIDIR}" $XD_BUILD_TYPE

    echo "BUILD_ONCE=1" >>$XLRDIR/BUILD_ONCE
    echo "OLDXLRDIR=${XLRDIR}" >>$XLRDIR/BUILD_ONCE
    echo "GIT_SHA_XCE=$(git rev-parse --short=8 --verify HEAD)" >>"$XLRDIR/BUILD_ONCE"
    cd "$XLRGUIDIR"
    echo "GIT_SHA_XD=$(git rev-parse --short=8 --verify HEAD)" >>"$XLRDIR/BUILD_ONCE"
    cd -
    echo "Created the BUILD_ONCE file"
    cp $XLRDIR/BUILD_ONCE ${DST_DIR}/${TAR_FILENAME}.vars

    echo "Tarring everything up..."
    tar -I pigz -cf - --exclude-vcs -C ${XLRDIR} . | split -b 200M - ${DST_DIR}/${TAR_FILENAME}.part
    for p in ${DST_DIR}/${TAR_FILENAME}.part*; do
        pn=$(basename $p)
        echo $pn >>${DST_DIR}/${TAR_FILENAME}.list
    done
    echo "Tar created at ${DST_DIR}"
}

extract_tar() {
    TMPDIR=$(mktemp -d -t build-once.XXXXXX)
    trap "rm -rf $TMPDIR" EXIT
    if [ -z "$1" ]; then
        say "No Build Once file was passed, aborting"
        usage
        exit 1
    fi

    if [[ $1 =~ http.*\.list$ ]]; then # path is a url to a list file
        wget $1 -P $TMPDIR
        # assumes files are at the same url/dir of file.list
        BASE_URL=${1%/*}
        cat ${TMPDIR}/*.list | sed "s#^#${BASE_URL}/#" | xargs -n 1 -P 4 wget -q -P $TMPDIR
        TAR_PATH_PATTERN="${TMPDIR}/*.part"
    else #assume the path if a directory, but test for the list to exist
        TAR_PATH_PATTERN=$1
    fi

    mkdir -p $DST_DIR
    echo "Extracting $TAR_PATH_PATTERN to $DST_DIR..."
    cat ${TAR_PATH_PATTERN}* | tar -I pigz -xf - -C $DST_DIR
    echo "Done extracting."
}

fix_paths() {
    if [ "${XLRDIR}" != "${DST_DIR}" ]; then
        say "XLRDIR (${XLRDIR}) is different than the DST_DIR ($DST_DIR), aborting"
        exit 1
    fi

    cd $XLRDIR
    set -a
    . BUILD_ONCE
    set +a

    # fix symlinks generated by link_qa macros & co
    if [ -z "$OLDXLRDIR" ]; then
        echo "OLDXLRDIR must be set"
        exit 1
    fi

    # fix ninja rules so we can run cmBuild sanity for backward compatibility
    sed -i -e "s,${OLDXLRDIR},${XLRDIR},g" ${XLRDIR}/buildOut/build.ninja

    find . -type l ! -exec test -e {} \; -print |
        while read LINK; do
            X="$(readlink $LINK)"
            #we have some broken links we don't wanna mess with
            if [[ $X == *${OLDXLRDIR}* ]] ; then
                X="${X#$OLDXLRDIR}"
                X="$XLRDIR$X"
                ln -sfn $X $LINK
            fi
        done

    # fix python venv
    rm -rf xcve
    mv ${XC_VENV_NAME} xcve
    for f in $(grep -lr "${OLDXLRDIR}" xcve/); do
        echo "Fixing $f"
        sed -i -e "s,${OLDXLRDIR},${XLRDIR},g" -e "s,/${XC_VENV_NAME},/xcve,g" $f
    done
}

if [ $# -lt 2 ]; then
    usage
    exit 1
fi

DST_DIR=$1
if [ ! -d "$DST_DIR" ]; then
    say "You passed \"${DST_DIR}\" as DST_DIR, but it does not exist or is not a directory"
    usage
    exit 1
fi
shift

case "$1" in
-w | --web)
    shift
    start_caddy
    ;;
-c | --create)
    shift
    create_tar
    ;;
-x | --extract)
    shift
    extract_tar $1
    ;;
-f | --fix)
    shift
    fix_paths
    ;;
-h | --help)
    usage
    exit 1
    ;;
*)
    usage
    exit 1
    ;;
esac

exit 0
