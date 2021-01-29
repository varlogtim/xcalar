#!/bin/bash

set -e

export BUILD_NUMBER="${BUILD_NUMBER:-1}"
export BUILD_ONCE_DIR="${BUILD_ONCE_DIR:-$(mktemp -d -t build-once.XXXXXX)}"
export BUILD_PROJECT=${BUILD_PROJECT:-XCE}

mkdir -p "${BUILD_ONCE_DIR}/${BUILD_NUMBER}"

bin/build-once.sh "${BUILD_ONCE_DIR}/${BUILD_NUMBER}" -c

exit $?