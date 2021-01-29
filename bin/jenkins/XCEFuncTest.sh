#!/bin/bash
set -e
set -x

RUN_FT=${RUN_FT:-true}

say () {
    echo >&2 "$*"
}

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
DIR="$(readlink -f $DIR)"

. $DIR/jenkinsUtils.sh

export XCE_LICENSEDIR=$XLRDIR/src/data
export XCE_LICENSEFILE=$XCE_LICENSEDIR/XcalarLic.key
export ExpServerd="true"
export XLRGUIDIR=$PWD/xcalar-gui
export XLRINFRADIR=$PWD/xcalar-infra

export PERSIST_COVERAGE_ROOT="${PERSIST_COVERAGE_ROOT:-/netstore/qa/coverage}"

if [ "$JOB_NAME" != "" ]; then
    export TIME_TO_WAIT_FOR_CLUSTER_START="${TIME_TO_WAIT_FOR_CLUSTER_START:-600}"
fi

TestList=$TestList
configFileBase="$XLRDIR/src/data/test.cfg"
XCE_CONFIG="$configFileBase-copy"
numNode=${NUM_NODES:-2}

TestArray=(${TestList//,/ })

# SDK-478 - build sanity now requires jupyter to run test_sqlmagic.py
JUPYTER_DIR=~/.jupyter
test -e $JUPYTER_DIR -a ! -h $JUPYTER_DIR && rm -rf $JUPYTER_DIR
ln -sfn ${XLRGUIDIR}/xcalar-gui/assets/jupyter/jupyter $JUPYTER_DIR
IPYTHON_DIR=~/.ipython
test -e $IPYTHON_DIR -a ! -h $IPYTHON_DIR && rm -rf $IPYTHON_DIR
ln -sfn ${XLRGUIDIR}/xcalar-gui/assets/jupyter/ipython $IPYTHON_DIR

trap "postReviewToGerrit" EXIT

rm -rf /var/tmp/xcalar-`id -un`/*
rm -rf /var/opt/xcalar/*
mkdir -p /var/tmp/xcalar-`id -un`/sessions

source $XLRDIR/doc/env/xc_aliases

set +e
sudo rm -rf /etc/xcalar/*
xclean
set -e

if [ "${BUILD_ONCE}" != "1" ]; then
    gitCheckoutIDL

    cmBuild clean

    if [ "$RUN_COVERAGE" = "true" ]; then
        echo "COVERAGE True"
        coverageDir=$(mktemp -d $(pwd)/coverage_XXXXX)
        echo "COVERAGE coverageDir: $coverageDir"
        export LLVM_PROFILE_FILE="$coverageDir/usrnode.%p.rawprof"
        cmBuild config debug -DCOVERAGE=ON
    else
        cmBuild config debug
    fi
    cmBuild qa
    # Build xcalar-gui so that expServer will run
    (cd $XLRGUIDIR && make dev)
fi

cd $XLRDIR

say "Shutdown cluster before start tests"
xc2 cluster stop

export GuardRailsArgs

date
cp $configFileBase $XCE_CONFIG
echo "$FuncTestParam" | cat >> $XCE_CONFIG

set +e
grep "^Constants.BufferCachePercentOfTotalMem=" "$XCE_CONFIG" &> /dev/null
if [ $? == 1 ]; then
    echo "Constants.BufferCachePercentOfTotalMem=$BufferCachePercentOfTotalMem" | cat >> $XCE_CONFIG
else
    sudo sed -i -e "s'Constants\.BufferCachePercentOfTotalMem=.*'Constants\.BufferCachePercentOfTotalMem=$BufferCachePercentOfTotalMem'" $XCE_CONFIG
fi

grep "^Constants.CtxTracesMode=" $XCE_CONFIG &> /dev/null
if [ $? == 1 ]; then
    if [ $CtxTracesMode != 0 ]; then
        echo "Constants.CtxTracesMode=$CtxTracesMode" | cat >> $XCE_CONFIG
    fi
else
    sudo sed -i -e "s'Constants\.CtxTracesMode=.*'Constants\.CtxTracesMode=$CtxTracesMode'" $XCE_CONFIG
fi
set -e

export XCE_CONFIG
export ExpServerd=false

find . -type f -name '*core*'
ps -ef | grep gdbserver | grep -v grep || true

if [ "$GuardRailsArgs" != "" ]
then
    echo "BUILD_DEBUG: Running Guardrails: $GuardRailsArgs"

    # Note that puppet should already take care of this and we need to avoid
    # sudo privileges.

    # Increase the mmap map count for GuardRails to work.
    map_count_var=$(cat /proc/sys/vm/max_map_count)
    if [ "$map_count_var" != 100000000 ]
    then
        echo 100000000 | sudo tee /proc/sys/vm/max_map_count
    fi

    # set this config param for GuardRails
    echo "Constants.NoChildLDPreload=true" >> $XCE_CONFIG

    # Needs to be added for cmBuild sanity to work
    echo "Constants.NoChildLDPreload=true" >> $configFileBase

    # Build GuardRails
    make -C xcalar-infra/GuardRails clean
    make -C xcalar-infra/GuardRails deps
    make -C xcalar-infra/GuardRails
fi

# ====================
# FuncTests ==========
# ====================
if [ "$RUN_FT" = "true" ]; then
    say "Functests: START =========="
    say "Functests: running xclean"

    xclean &> /dev/null

    say "Functests: starting cluster"
    if [ "$GuardRailsArgs" != "" ]; then
        xc2 cluster start --num-nodes "${numNode}" --guardrails-args "$GuardRailsArgs"
    else
        xc2 cluster start --num-nodes "${numNode}"
    fi
    say "Functests: cluster ready"

    say "Functests: starting tests"
    for Test in "${TestArray[@]}"; do
        testOut="$Test.out"

        say "FUNCTEST_TEST_START: '$Test'"
        time xccli -c "functests run --allNodes --testCase $Test" | tee "$testOut"
        say "FUNCTEST_TEST_END: '$Test'"
        if ! grep -q "Success" "$testOut"; then
            if ! grep -q "NumTests: 0" "$testOut"; then
                say "Functests: '$Test' failed:"
                cat "$testOut"
                rm -f "$testOut"
                exit 1
            else
                say "Functests: WARNING! '$Test' does not exist in the test suite"
            fi
        fi
        rm -f "$testOut"
    done
    say "Functests: tests complete"

    # The funcTests are over. Don't just die on first failure now
    set +e

    # TODO need to check the return for stat in case of regression
    say "Functests: serialization stats"
    xccli -c "stats 0" | grep XdbMgr

    say "Functests: stopping cluster"

    xc2 cluster stop

    say "Functests: DONE =========="
fi

# ====================
# Sanity =============
# ====================
if [ "$RUN_SANITY" = "true" ]; then
    say "Sanity: START =========="
    say "Sanity: running xclean"
    xclean
    say "Sanity: starting tests"
    cmBuild sanity
    tmpRet=$?
    say "Sanity: tests complete"
    say "Sanity: cmBuild sanity returned $tmpRet"
    ret=$(($ret + $tmpRet))
    say "Sanity: DONE =========="
fi

# ====================
# Sqldf_sanity =======
# ====================
if [ "$RUN_SQLDF_SANITY" = "true" ]; then
    set -e
    set -x
    say "Sqldf_sanity: START =========="
    onExit() {
        local retval=$?
        set +e
        (cd "$SPARK_DOCKER_DIR" && make clean)

        if [[ $retval != 0 ]]
        then
            genBuildArtifacts
            say "Sqldf_santiy: build artifacts copied to ${NETSTORE}/${JOB_NAME}/${BUILD_ID}"
        fi

        (cd src/sqldf/ && git clean -fxd)
        exit $retval
    }

    test_jdbc() {
        if [ "$RUN_COVERAGE" = "true" ]; then
            # Pass --cov-timeout option to adjust underlying timeouts to account
            # for SLOW coverage build. See ENG-5183.
            #
            # Same SLOW coverage build plays havoc with timing at some level
            # and coughs up thrift transport broken pipe errors.
            # No real need for spark when running for coverage numbers so
            # take it out of the picture.  See ENG-4937.
            ./src/sqldf/tests/test_jdbc.py $JDBC_TEST_OPTS --cov-timeout 50000 -l -p "$XLRGUIDIR/assets/test/json/" "$@"
        else
            ./src/sqldf/tests/test_jdbc.py $JDBC_TEST_OPTS -l -p "$XLRGUIDIR/assets/test/json/" -S $SPARK_IP "$@"
        fi
    }

    if [ "$RUN_COVERAGE" = "false" ]; then
        #
        # Coverage doesn't do Spark verify, so don't bother with this.
        #
        JAVA_HOME=$(java_home.sh)
        export JAVA_HOME
        SPARK_DOCKER_DIR="$XLRDIR/docker/spark/master"

        (cd "$SPARK_DOCKER_DIR" && make rm && make run)
        trap onExit EXIT

        SPARK_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' spark-master-jdbc)
        echo "Spark JDBC server at $SPARK_IP"
    fi

    # Cleanup the sqldf env
    (cd src/sqldf/ && git clean -fxd)

    # Extract the latest sqldf from the netstore
    mkdir -p $XLRDIR/src/sqldf/sbt/target/
    set -a
    . ${XLRDIR}/src/3rd/spark/BUILD_ENV
    set +a
    ${XLRDIR}/bin/download-sqldf.sh $XLRDIR/src/sqldf/sbt/target/xcalar-sqldf.jar

    say "Sqldf_sanity: running xclean"
    xclean
    say "Sqldf_sanity: starting the cluster"
    xc2 cluster start --num-nodes 1

    LOGIN_JSON='{"xiusername": "admin", "xipassword": "admin"}'
    curl -c cookiejar -H "Content-Type: application/json" -X POST -d "$LOGIN_JSON" "http://localhost:12124/login" || true

    if [ "$RUN_COVERAGE" = "true" ]; then
        # absence of -u forces launch of new xcalar-sqldf.jar instance
        # -r sets read timeout of that instance high to account for coverage overhead
        # -l leaves new instance running when done (for subsequent testing)
        # See ENG-4937 for why.
        (cd src/sqldf/tests && ./test_sqldf.sh -r 600000 -l)
    else
        # -u uses existing xcalar-sqldf.jar instance
        (cd src/sqldf/tests && ./test_sqldf.sh -u)
    fi

    say "Sqldf_sanity: starting tests"
    test_jdbc

    for jdbc_test_daily in $JDBC_TEST_DAILY; do
        say "Sqldf_sanity: starting jdbc_test_daily $jdbc_test_daily"
        test_jdbc -t $jdbc_test_daily
    done
    say "Sqldf_sanity: tests complete"

    # The sql sanity test are done. Don't fail on the first failure for now
    set +e
    say "Sqldf_sanity: stopping cluster"
    xc2 cluster stop

    say "Sqldf_sanity: DONE =========="
fi

if [ "$RUN_COVERAGE" = "true" ]; then
    set +e
    echo "RUN_COVERAGE is TRUE: post-processing coverage data"

    # Create persistent storage for our coverage data
    outputDir=${PERSIST_COVERAGE_ROOT}/${JOB_NAME}/${BUILD_NUMBER}
    mkdir -p "$outputDir"
    echo "COVERAGE outputDir: $outputDir"

    # Save the raw profile data into persistent storage
    outRawDir="${outputDir}/rawprof" # raw profile data go here
    mkdir -p "$outRawDir"
    echo "COVERAGE outRawDir: $outRawDir"
    echo "COVERAGE stashing raw profile data"
    find ${coverageDir} -type f ! -size 0 | xargs -I '{}' cp '{}' ${outRawDir}

    srcDir="${outputDir}/src"
    mkdir -p "$srcDir"
    echo "COVERAGE srcDir: $srcDir"
    echo "COVERAGE stashing sources for coverage report"
    cp -ar --parents /opt/xcalar/include/* ${srcDir}/
    cp -ar --parents `pwd`/buildOut/src/include/* ${srcDir}/
    cp -ar --parents `pwd`/buildOut/src/lib/* ${srcDir}/
    cp -ar --parents `pwd`/src/* ${srcDir}/


    # Save the instrumented binaries to persistent storage
    usrnodePath="$(readlink -f $(which usrnode))"
    echo "COVERAGE usrnodePath: $usrnodePath"
    outBinDir="${outputDir}/bin" # binaries go here
    mkdir -p $outBinDir
    echo "COVERAGE outBinDir: $outBinDir"
    echo "COVERAGE stashing binaries"
    cp "$usrnodePath" "$outBinDir"

    chmod -R o+rx "$outputDir" # Play nice with others :)
fi

exit $ret
