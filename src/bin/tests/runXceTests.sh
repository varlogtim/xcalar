#!/bin/bash

NumNodes=3
TestDuration=60
DfSeed=100
DfFiles="op_df-*.tar.gz"
DatasetSeed=200

exitHandler()
{
    if [[ ! -z "$GuardRailsArgs" ]]; then
        rm -f grargs.txt
    fi
    xc2 cluster stop
}

cleanOut()
{
    local exitCode=$1
    echo "*** Begin $0 cleanout"
    echo "*** Stop cluster"
    xc2 cluster stop
    tmpExitCode=$?
    if [ "$tmpExitCode" != "0" ]; then
        echo "Failing $0 because of unclean shutdown" 1>&2
        exit $tmpExitCode
    fi

    echo "*** Remove DF files $DfFiles"
    rm $DfFiles
    echo "*** $0 ENDING ON `/bin/date`"
    exit $exitCode
}

trap exitHandler EXIT

TMPDIR=$(mktemp -d -t runXceTests.XXXXXX)

echo "Begin $0 on `/bin/date`; output: $TMPDIR"

TMP="$TMPDIR/xceTests.out"

xc2 cluster start --num-nodes $NumNodes
exitCode=$?
if [ "$exitCode" != "0" ]; then
    echo "Failing $0 because failed to start cluster" 1>&2
    exit $exitCode
fi

# Choose seed here to make the tests deterministic
echo "*** $0 Run initial test cycle"
python $XLRDIR/src/bin/tests/xceTests/test_xce.py --test_name test_operators --df_seed $DfSeed --dataset_seed $DatasetSeed  --duration $TestDuration 2>&1 | tee "$TMP"
exitCode=${PIPESTATUS[0]}
if [ "$exitCode" != "0" ]; then
    cleanOut $exitCode
    exit $exitCode
fi

# Re-run the test to allow test to restore from saved DFs
echo "*** $0 Run DF restore test cycle"
python $XLRDIR/src/bin/tests/xceTests/test_xce.py --test_name test_operators --df_seed $DfSeed --dataset_seed $DatasetSeed  --duration $TestDuration 2>&1 | tee "$TMP"
if [ "$exitCode" = "0" ]; then
    echo "*** $0 passed"
fi

cleanOut $exitCode
