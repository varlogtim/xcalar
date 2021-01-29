#!/bin/bash

exitHandler()
{
    if [[ ! -z "$GuardRailsArgs" ]]; then
        rm -f grargs.txt
    fi
    xc2 cluster stop
}

trap exitHandler EXIT

TMPDIR=$(mktemp -d -t localSystemTest.XXXXXX)

echo "Begin $0 on `/bin/date`; output: $TMPDIR"

TMP="$TMPDIR/localSystemTest.out"

xc2 cluster start --num-nodes 3
exitCode=$?
if [ "$exitCode" != "0" ]; then
    echo "Failing $0 because failed to start cluster" 1>&2
    exit $exitCode
fi

python $XLRDIR/src/bin/tests/systemTests/runTest.py -n 1 -i localhost:18552 -t testConfig -w -c "$XLRDIR/bin" 2>&1 | tee "$TMP"
exitCode=${PIPESTATUS[0]}

xc2 cluster stop
tmpExitCode=$?
if [ "$tmpExitCode" != "0" ]; then
    echo "Failing $0 because of unclean shutdown" 1>&2
    exit $tmpExitCode
fi

if [ "$exitCode" = "0" ]; then
    echo "*** $0 passed"
fi

echo "*** $0 ENDING ON `/bin/date`"
exit $exitCode
