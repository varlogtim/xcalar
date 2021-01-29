#!/bin/bash
set -e
set -x

# Run test_jdbc.py against the given host/port.
# Assumes Xcalar server managed elsewhere.

say () {
    echo >&2 "$*"
}

say "TestJdbc START ===="

say "TestJdbc validate inputs ===="
if [ -z $JDBC_SERVER_HOST ]; then
    say "ERROR: JDBC_SERVER_HOST cannot be empty"
    exit 1
fi

if [ "$JDBC_SERVER_HOST" = "localhost" ]; then
    say "ERROR: localhost not supported"
    exit 1
fi

if [ -z $JDBC_SERVER_PORT ]; then
    say "ERROR: JDBC_SERVER_PORT cannot be empty"
    exit 1
fi

if [ -z $XCALAR_USER ]; then
    say "ERROR: XCALAR_USER cannot be empty"
    exit 1
fi

if [ -z $XCALAR_PASS ]; then
    say "ERROR: XCALAR_PASS cannot be empty"
    exit 1
fi

if [ -z $API_PORT ]; then
    say "ERROR: API_PORT cannot be empty"
    exit 1
fi

if [ -z "$TEST_JDBC_OPTIONS" ]; then
    say "ERROR: TEST_JDBC_OPTIONS cannot be empty"
    exit 1
fi

say "TestJdbc build ===="
cd $XLRDIR
cmBuild clean
cmBuild config debug
# XXXrs - Tech Debt
# Only want to get the python in place to run test_jdbc.py but can't
# figure out the right make target in a timely manner.
# "xce" is (very) heavyweight but works reliably, so just use it
# for now/(ever?).
cmBuild xce

say "TestJdbc run test_jdbc.py ===="

if [ -z "$PERF_PREFIX" ]; then
    perf_options=""
else
    export NETSTORE="${NETSTORE:-/netstore/qa/jenkins}"
    RESULTS_DIR="${NETSTORE}/${JOB_NAME}/${BUILD_ID}"
    mkdir -p "$RESULTS_DIR"
    perf_options="-o ${RESULTS_DIR}/${PERF_PREFIX}"
fi

host_options="-H $JDBC_SERVER_HOST --jdbc-port $JDBC_SERVER_PORT"
user_options="-a $API_PORT -U $XCALAR_USER -P $XCALAR_PASS"
./src/sqldf/tests/test_jdbc.py $TEST_JDBC_OPTIONS $host_options $user_options $perf_options

say "TestJdbc END ===="
