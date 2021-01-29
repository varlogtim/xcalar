#!/bin/bash

set -x

DEFAULT_QA_DIR=`readlink -f "$BUILD_DIR/src/data/qa"`
export XcalarQaDatasetPath="${XcalarQaDatasetPath:-$DEFAULT_QA_DIR}"
export XCE_CONFIG="${XLRPY_CFG_PATH:-$XLRDIR/src/bin/usrnode/test-config.cfg}"
export XLR_PYSDK_VERIFY_SSL_CERT="${XLR_PYSDK_VERIFY_SSL_CERT:-false}"
export ENABLE_SLOW_TESTS="${ENABLE_SLOW_TESTS:-false}"

TESTDIR="$XLRDIR/src/bin/tests/pyTestNew"

function fail
{
    echo >&2 "$*"
    exit 1
}

SMOKETESTS=false

for arg in "$@"; do
  case "$arg" in
    --smoke) SMOKETESTS=true;;
  esac
done
# get rid of --smoke from "$@"
set -- "${@//--smoke/}"

pyMarks="not nolocal"

# Unfortunately, our internal cloudera HDFS is down, so no way to run these tests
pyMarks="$pyMarks and not odbc"
#echo "quit" | isql -v Hive hiveuser Welcome1
#ret=$?
#if [ "$ret" != "0" ]; then
#    # odbc not set up correctly, so ignore pyTests that require it
#    pyMarks="$pyMarks and not odbc"
#fi
if [ -n "$container" ] || test -e /run/systemd/container || /is_container 2>/dev/null; then
    pyMarks="$pyMarks and not nocontainer"
fi

java -jar "$XLRDIR/bin/parquet-tools-1.8.2.jar" --help 2>/dev/null
ret=$?
if [ "$ret" != "0" ]; then
    # parquet-tools not available, so ignore pyTests that require it
    pyMarks="$pyMarks and not parquetTools"
fi

# Smoketest is a subset of all the tests that do not manage
# their own clusters. This is meant for developers to run on their
# local dev machine and should complete in less than 1 hour.
if [ $SMOKETESTS == true ]; then
    pyMarks="$pyMarks and not last"
fi

if [ "$ENABLE_SLOW_TESTS" = "false" ]; then
    pyMarks="$pyMarks and not slow"
fi

start_minio(){
    echo "start minio ..."
    (cd $XLRDIR/docker/minio && docker-compose up -d)
}

stop_minio(){
    echo "Stop minio ..."
    (cd $XLRDIR/docker/kafka && docker-compose down)
}

start_kafka(){
    echo "start kafka ..."
    (cd $XLRDIR/docker/kafka && make up)
}

stop_kafka(){
    echo "Stop kafka ..."
    (cd $XLRDIR/docker/kafka && make down)
}

# Stop any previous lingering instances of minio
stop_minio

#if ! start_minio; then
#    echo >&2 "Minio failed to come up"
#    pyMarks="$pyMarks and not minio"
#fi

# Stop any previous lingering instances of kafka
stop_kafka

if ! start_kafka; then
    echo >&2 "Kafka failed to come up"
    pyMarks="$pyMarks and not kafka"
fi

# XXX remove '--disable-warnings' once protobuf is above version 3.3
cd "$TESTDIR" && python -m pytest -vv -rw -x --durations=0 --disable-warnings --randomly-dont-reorganize --cov=${XLRDIR}/src/bin/sdk --cov-config ${XLRDIR}/.coveragerc --cov-append --cov-report html:${BUILD_DIR}/pytest_coverage_html --cov-report term -m "$pyMarks" "$@" .
rc=$?

stop_kafka
stop_minio
exit $rc
