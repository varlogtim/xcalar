#!/bin/bash
set -e
set -x

say() {
    echo >&2 "$*"
}

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
DIR="$(readlink -f $DIR)"
. $DIR/jenkinsUtils.sh

export XLRGUIDIR=$PWD/xcalar-gui
export XLRINFRADIR=$PWD/xcalar-infra
configFile="$XLRDIR/src/data/test.cfg"

onExit() {
    local retval=$?

    set +e
    docker rm -f -v $PG_CONTAINER

    genBuildArtifacts
    corefileExists=$?

    say "${JOB_NAME} exited with return code $retval"
    say "genBuildArtifacts returned $corefileExists"
    say "Build artifacts copied to ${NETSTORE}/${JOB_NAME}/${BUILD_ID}"
    set -e

    retval=$((retval + corefileExists))
    exit $retval
}

setup_postgres() {
    echo "Set up Posgresql docker container"
    if [ ! "$(docker ps -q -f name=$PG_CONTAINER)" ]; then
        if [ "$(docker ps -aq -f status=exited -f name=$PG_CONTAINER)" ]; then
            # cleanup
            docker rm -f -v $PG_CONTAINER || true
        fi
        # start container
        docker run -d --name $PG_CONTAINER -e POSTGRES_PASSWORD=$PG_PASS -e POSTGRES_USER=$PG_USER -e POSTGRES_DB=$PG_DB -p $PG_PORT:5432 postgres
    else
        # reuse existing container
        echo "Posgresql docker container $PG_CONTAINER already exists, reusing.."
    fi
}

test_imd() {
    if [ "$RunImdPostgresql" = "true" ]; then
        setup_postgres
        echo "Run IMD tests with  verification against Posgresql"
        ./src/bin/tests/imdTests/test_imd.py --db $PG_DB --dbHost ${HOSTIP:-localhost} --dbPort $PG_PORT --dbUser $PG_USER --dbPass $PG_PASS "$@"
    fi

    if [ "$RunImdTpch" = "true" ]; then
        echo "Run IMD tests against tpch working set with algorithmic data verification"
        ./src/sqldf/tests/test_jdbc.py -t test_tpch_xd_dataflows --xcSess tpchSess_Merge -p src/sqldf/tests/testPlans/tpch --SF 1 -w 1 --testMergeOp
    fi

    if [ "$RunImdTpcds" = "true" ]; then
        echo "Run IMD tests against tpcds workking set with algorithmic data verification"
        ./src/sqldf/tests/test_jdbc.py -t test_tpcds_xd_dataflows --xcSess tpcdsSess_Merge -p src/sqldf/tests/testPlans/tpcds --SF 1 -w 1 --testMergeOp
    fi
}

trap onExit EXIT

cd $XLRDIR
source doc/env/xc_aliases

# XCE debug build
cmBuild clean
cmBuild config debug
cmBuild xce

# Build xcalar-gui so the expServer can run
(cd $XLRGUIDIR && make dev)

xclean

GuardRailsArgs=${GuardRailsArgs:-}
declare -a XC2_ARGS=("cluster" "start" "--num-nodes" ${NUM_NODES})

if [ "$GuardRailsArgs" != "" ]; then
    echo "BUILD_DEBUG: Running Guardrails: $GuardRailsArgs"

    # Increase the mmap map count for GuardRails to work
    if [ -n "$container" ] || /is_container 2>/dev/null; then
        :
    else
        echo 100000000 | sudo tee /proc/sys/vm/max_map_count
    fi

    # set this config param for GuardRails
    echo "Constants.NoChildLDPreload=true" >>$configFile

    # Build GuardRails
    make -C xcalar-infra/GuardRails clean
    make -C xcalar-infra/GuardRails deps
    make -C xcalar-infra/GuardRails

    XC2_ARGS+=("--guardrails-args" "$GuardRailsArgs")
fi

xc2 "${XC2_ARGS[@]}"

export XLR_PYSDK_VERIFY_SSL_CERT=false

userOptions=" --restoreRate $RESTORE_RATE --snapshotRate $SNAPSHOT_RATE --validationRate $VALIDATION_RATE"
if [ "$BOUNDARY_TESTS" = true ]; then
    userOptions+=" --run_boundary_tests"
fi
test_imd $userOptions

xc2 cluster stop
