#!/bin/bash

#
# This script wraps pyFeatureTests.py. It setups the necessary environment
# for running these tests from within a Xcalar repository and starts the
# Xcalar backend.
#

set -x

# Reduced from 5 to 3.  5 may be insufficient on smaller hosts.  See SDK-640
DEFAULT_NUM_NODES=3
USAGE="$0 [--numNodes/-n <numNodes> [--debugXcalar]]"

TESTDIR="$XLRDIR/src/bin/tests/pyTest"

export ENABLE_SLOW_TESTS="${ENABLE_SLOW_TESTS:-false}"
export XCE_CONFIG="${XLRPY_CFG_PATH:-$XLRDIR/src/bin/usrnode/test-config.cfg}"
export XLRPY_MGMTD_URL="${XLRPY_MGMTD_URL:-http://localhost:9090/thrift/service/XcalarApiService/}"
export XLR_PYSDK_VERIFY_SSL_CERT="${XLR_PYSDK_VERIFY_SSL_CERT:-false}"

DEFAULT_QA_DIR=`readlink -f "$BUILD_DIR/src/data/qa"`
export XcalarQaDatasetPath="${XcalarQaDatasetPath:-$DEFAULT_QA_DIR}"

function fail
{
    echo >&2 "$*"
    exit 1
}

# Parse arguments.
export XcalarQaNumNodes=$DEFAULT_NUM_NODES
startArgs=""
if [ "$JOB_NAME" != "" ]; then
    # Have a crazy timeout for Jenkins
    export TIME_TO_WAIT_FOR_CLUSTER_START="${TIME_TO_WAIT_FOR_CLUSTER_START:-99999}"
fi

managedCluster=true
pyMarks="not nolocal"

while [ $# -gt 0 ]; do
    opt="$1"
    shift
    case "$opt" in
        -n|--numNodes)
            test -z "$1" && fail "$USAGE\nNeed to specify numNodes."
            export XcalarQaNumNodes="$1"
            shift
            ;;
        -g|--guardrails)
            startArgs="--guard-rails ${startArgs}"
            ;;
        -u|--unmanaged)
            # Manage the cluster lifecycle outside script (ie use existing
            # usrnode instances and don't kill them)
            managedCluster=false
            ;;
        -m|--marks)
            # pyMarks
            pyMarks="$1"
            shift
            ;;
        --)
            # Pass everything after -- along to py.test.
            break
            ;;
        *)
            fail "$USAGE\nUnknown option: $opt."
            ;;
    esac
done

# XXX This should be done by usrnode.
$managedCluster && killall -9 childnode

# Launch usrnode and mgmtd. It's the test's responsibility for getting rid of these.

$managedCluster && xc2 -vv cluster start --num-nodes ${XcalarQaNumNodes} ${startArgs}

if [ $? -ne 0 ]; then
    $managedCluster && fail
fi

# Execute tests.

# Check if odbc connection to hive metastore is setup.
# To setup odbc connection to hive metastore:
# 1) Follow instructions in bin/install_unixOdbc.sh to set up MySQL ODBC connector
# 2) Install pyodbc
# 3) Add the following

# /etc/odbc.ini
#[Hive]
#Description     = This Data Source connects to the Hive Metastore
#Driver      = myodbc_mysql
#Server      = cloudera-hdfs-dl-kvm-01.int.xcalar.com
#Username        = hiveuser
#Password        = Welcome1
#Port        = 3306
#Socket      = /var/lib/mysql/mysql.sock
#Database        = metastore_db
#ReadOnly        = no
#Option      = 3

# /etc/odbcinst.ini
#[myodbc_mysql]
#Description     = MySql ODBC driver
#Driver      = /usr/lib/x86_64-linux-gnu/odbc/libmyodbc.so
#Setup       = /usr/lib/x86_64-linux-gnu/odbc/libodbcmyS.so
#UsageCount      = 3


echo "quit" | isql -v Hive hiveuser Welcome1
ret=$?
if [ "$ret" != "0" ]; then
    # odbc not set up correctly, so ignore pyTests that require it
    pyMarks="$pyMarks and not odbc"
fi

if ! java -jar "$XLRDIR/bin/parquet-tools-1.8.2.jar" --help >/dev/null 2>&1; then
    # parquet-tools not available, so ignore pyTests that require it
    pyMarks="$pyMarks and not parquetTools"
fi

if [ "$ENABLE_SLOW_TESTS" = "false" ]; then
    pyMarks="$pyMarks and not slow"
fi

# "--cov-report= " note that we leave the report empty; we want this to be
# combined with pyTestNew before reporting the coverage results. Note we do NOT
# apply the --cov-append option, since we want to start fresh here, since this
# runs before pyTestNew
(cd $TESTDIR && python -m pytest -vv -rw -x --durations=0 --randomly-dont-reorganize --cov=${XLRDIR}/src/bin/sdk --cov-config ${XLRDIR}/.coveragerc --cov-report= -m "$pyMarks" "$@" .)
ret=$?

if $managedCluster; then
    xc2 cluster stop
fi

exit $ret
