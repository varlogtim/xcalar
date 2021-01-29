#!/bin/bash

VERSION="${VERSION:-3.6.2}"
PREFIX=/opt/xcalar

usage () {
    echo "$0 [-i iteration] [-V python-version (default: $VERSION)] [-v version] [-p prefix (default: $PREFIX)] [-x package-prefix (default: $PACKAGE_PREFIX)] package-name"
    exit 1
}

while getopts "hi:v:p:x:V:" opt; do
    case "$opt" in
        h) usage;;
        i) ITERATION="$OPTARG";;
        v) PACKAGE_VERSION="$OPTARG";;
        p) PREFIX="$OPTARG";;
        x) PACKAGE_PREFIX="$OPTARG";;
        V) VERSION="$OPTARG";;
        \?) echo >&2 "Invalid option -$OPTARG"; exit 1;;
        :) echo >&2 "Option -$OPTARG requires an argument."; exit 1;;
    esac
done

VER="${VERSION:0:3}"  # eg, 3.6
NVER="${VERSION:0:1}${VERSION:2:1}" # eg, 36
PACKAGE_PREFIX=${PACKAGE_PREFIX:-xcalar-python${NVER}}

ITERATION="${ITERATION:-20}"

shift $(( $OPTIND - 1 ))

test -e /etc/redhat-release && PACKAGE_TYPE=rpm || PACKAGE_TYPE=deb

# Rename all helper scripts (eg, faker) to be versioned (faker-3.6)

fpm -s python -t ${PACKAGE_TYPE} \
    --python-bin ${PREFIX}/bin/python${VER} --python-pip ${PREFIX}/bin/pip${VER} --python-easyinstall ${PREFIX}/bin/easy_install-${VER} \
    --python-install-bin ${PREFIX}/scripts/python${VER} --python-install-data ${PREFIX}/share/python${VER} \
    --python-package-prefix ${PACKAGE_PREFIX} --iteration ${ITERATION} -v ${PACKAGE_VERSION} -f "$@"
