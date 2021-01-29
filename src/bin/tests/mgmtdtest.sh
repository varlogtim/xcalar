#!/bin/bash

#
# mgmtdtest.sh runs the javascript mgmtd tests. Source for these tests is in
# MgmtTest.js. This script loads test dependencies and prepares the test
# environment. The PhantomJS javascript interpreter is then invoked to run the
# test.
#

DIR=`dirname ${BASH_SOURCE[0]}`

# Used by update license test
export XCE_QALICENSEDIR=$(readlink -f "$XLRDIR/src/bin/tests")
export XCE_LICENSEDIR=${XCE_LICENSEDIR:-$XLRDIR/src/data}
export XCE_LICENSEFILE=${XCE_LICENSEFILE:-$XCE_LICENSEDIR/XcalarLic.key}

# Constants you can modify
numNodes=3

# The config file gets modified by the setConfigParm test
# so build has made a changeable copy.
configFile="$DIR/test-config-mgmtd.cfg"
if grep -q "Constants\.TestMode" "$configFile"; then
    sed -i -e "s/Constants\.TestMode=false/Constants\.TestMode=true/I" "$configFile"
else
    echo "Constants.TestMode=true" >> "$configFile"
fi

pathToQaDatasets=$(readlink -f "$BUILD_DIR/src/data/qa")

testDebugPort=9000

. utils.sh
. qa.sh
. nodes.sh

TMPDIR=`mktemp /tmp/mgmtdtest.XXXXXX`
rm -f $TMPDIR
mkdir $TMPDIR

echo "Begin mgmtdtest on `/bin/date`; output: $TMPDIR"

TMP2=$TMPDIR/xcmgmtd.out
TMP3=$TMPDIR/mgmttestactual.js.out

exitCode=0
tmpExitCode=0

# Parse command line args.
debugTest=0
for arg in "$@"; do
    if [ "$arg" = "--debugTest" ]; then
        debugTest=1
    elif [ "$arg" = "--debugXcalar" ]; then
        interactiveMode=1
    fi
done

function fail
{
    echo >&2 "$*"
    exit 1
}

if ! xc2 -vv cluster start --num-nodes $numNodes; then
    fail
fi

userBreakpoint "All nodes are up."

testArgs=""
phantomArgs="--web-security=false"
if [ "$debugXcalar" != "0" ]
then
    # Skip the test which starts usrnodes (because we already did that to attach
    # debuggers to them).
    testArgs="nostartnodes $testArgs"
fi
if [ "$debugTest" != "0" ]
then
    phantomArgs="--remote-debugger-port=${testDebugPort} $phantomArgs"
fi

export TMP_DIR="$TMPDIR"
export QATEST_DIR="$pathToQaDatasets"
export MGMTDTEST_DIR=$DIR
/usr/bin/phantomjs $phantomArgs $DIR/mgmttestdriver.js $testArgs 2>&1
tmpExitCode=$?
if [ $tmpExitCode -ne "0" ]; then
    echo "*** mgmtdtest failed due to mgmttestactual.js nonzero return!" 1>&2
    echo "xcmgmtd output:" 1>&2
    cat $TMP2 1>&2
    exitCode=1
fi
export MGMTDTEST_DIR=

echo "shutdown cluster"
xc2 cluster stop
tmpExitCode=$?
if [ "$exitCode" = "0" -a "$tmpExitCode" != "0" ]; then
    echo "Failing mgmtdtest because of unclean shutdown" 1>&2
    exitCode=$tmpExitCode
fi

# XXX We should be comparing outputs of $TMP3 with expected output rather than
# just validating that all calls were successful, which we are doing now

# Levi: I think this is unnecessary, now phantom will always exit with the correct returnValue
# if [ "$exitCode" = "0" ]; then
#     echo "Parsing mgmttestactual.js output..."
#     grep -q failed $TMP3
#     if [ "$?" = "0" ]
#     then
#         echo "*** mgmtdtest (MgmtTest.js) failed:" 1>&2
#         cat $TMP3 1>&2
#         exitCode=1
#     fi

if [ "$exitCode" = "0" ]; then
    echo "*** mgmtdtest passed."
    #rm -rf $TMPDIR
else
    for filn in `/bin/ls -1 $TMPDIR`
    do
        grep -q -i assert $TMPDIR/$filn
        if [ "$?" = "0" ]
        then
            echo "*** assertion failure detected $filn: `grep -i assert $TMPDIR/$filn`"
        fi
    done
    echo "*** Test output is left in $TMPDIR for your examination"
fi

echo "*** mgmtdtest.sh ENDING ON `/bin/date`"

exit $exitCode
