#!/bin/bash
set -e

NAME=python
UPNAME=Python
VERSION=${1?Must specify Python version to build}
ITERATION="${ITERATION:-$BUILD_NUMBER}"
if [ -z "${ITERATION}" ]; then
    echo >&2 "Need to set ITERATION or BUILD_NUMBER"
    exit 1
fi

if [ -n "$JENKINS_URL" ]; then
    git clean -fxd >/dev/null
fi

safe_curl () {
    curl -4 --location --retry 20 --retry-delay 3 --retry-max-time 60 "$@"
}

build_python () {
    crun ${1} bash -c "source ~/.*profile && ITERATION=$BUILD_NUMBER bin/install-python.sh ${VERSION}" 2>&1 | sed  's/^/'${1}': /g'
    return ${PIPESTATUS[0]}
}

onterm () {
    set +e
    local rc=$1
    local pid=
    for pid in "${PIDS[@]}"; do
        kill -0 $pid 2>/dev/null && kill -INT $pid
    done
    sleep 2
    for pid in "${PIDS[@]}"; do
        kill -0 $pid 2>/dev/null && kill -TERM $pid
    done
    local killdps=$(comm -2 -3 <(docker ps -q | sort) <(echo "$DPS"))
    test -z "$killdps" || docker kill $killdps
    return $rc
}

ITERATION="${ITERATION:-$BUILD_NUMBER}"
(cd docker && make)
PIDS=()
DPS=$(docker ps -q | sort)

safe_curl -fsS https://www.python.org/ftp/${NAME}/${VERSION}/${UPNAME}-${VERSION}.tgz -O
for OSID in amzn1 ub14 el7; do
    build_python ${OSID}-build &
    PIDS+=($!)
done
trap 'onterm $?' EXIT INT QUIT ABRT HUP TERM
set +e
failed=0
for pid in "${PIDS[@]}"; do
    wait $pid
    rc=$?
    if [ $rc -ne 0 ]; then
        echo >&2 "ERROR($rc): Build failed"
        failed=1
    fi
done
trap '' EXIT INT QUIT ABRT HUP TERM
exit $failed
