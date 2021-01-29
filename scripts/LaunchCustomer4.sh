#!/bin/bash
#set -x # chatty!

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export XLRDIR="$(cd "$DIR"/.. && pwd)"
export PATH="$XLRDIR"/bin:"$PATH"

. xcalar-sh-lib

if enter_container "$@"; then
    echo >&2 "[$container] >>> $0 $*"
else
    echo >&2 "ERROR: Couldn't enter container"
    exit 1
fi

init() {
    # -------------------------
    # Default
    # -------------------------
    export XLRGUIDIR="${XLRGUIDIR:-${HOME}/xcalar-gui}"
    export XLRINFRADIR="${XLRINFRADIR:-${HOME}/xcalar-infra}"
    export PATH=$XLRINFRADIR/bin:$XLRDIR/bin:/opt/xcalar/bin:$PATH
    . xcsetenv

    # -------------------------
    # install Default
    # -------------------------
    DCC_VOLUMES=()

    # -------------------------
    # uninstall Default
    # -------------------------
    SAVE_LOGS=''

    # -------------------------
    # data_gen Default
    # -------------------------
    NODES=3
    XCALARHOST=${HOSTIP:-localhost}
    APIPORT=${XCE_HTTPS_PORT:-18443}
    XCALARUSER=admin
    XCALARPASS=admin
    SESSIONNAME=data_gen
    BASESIZE=100000
    DELTASIZE=10000
    NUMBATCHES=-1
    KAFKASERVER="${HOSTIP}:9092"

    CLEANUP=''
    DELTAS_ONLY=''
    LOCAL=''
    # -------------------------
    # test_rig_main Default
    # -------------------------
    APP_CONFIG='/netstore/datasets/customer4/simulationRig/appConfig'
    SNAPSHOT_PATH='/tmp'
    BASE_DATA_PATH='/tmp' # XXXrs
    PREMISES=2
    ACTION_STOP='--action stop'
}

menu() {
    cat <<EOF
Usage: $0  [OPTION]
       $0  [OPTION] [ARG]

   [OPTION]
      -A, --ACTION        [ install | uninstall | start | stop | run_data_gen | run_test_rig | stop_test_rig ]

   [ARG]
      -i, --image_tag     Image Tag (default: latest )
      -n, --NODES         Number of nodes (default: 3)
      -p, --PREMISES      Number of PREMISES (default: 2)
      -H, --XCALARHOST    Host of the Xcalar cluster (default: localhost)
      -a, --APIPORT       Xcalar API port (18443 for some dockers) (default: 18443)
      -U, --XCALARUSER    XCALARUSER (default:admin)
      -P, --XCALARPASS    XCALARPASS (default:admin)
      -s, --SESSIONNAME   SESSIONNAME (default:data_gen)
      -b, --BASESIZE      BASESIZE (default:100000)
      -d, --DELTASIZE     SESSIONNAME (default:10000)
      -m, --NUMBATCHES    NUMBATCHES (default:-1)
      -k, --KAFKASERVER   KAFKASERVER (default:<local_host_ipaddr>:9092)
      -v, --DCC_VOLUMES   Additional volumes to mount to dcc cluster (default: ())
      -L, --SAVE_LOGS     Copy dcc cluster logs to this directory (default: none)
      -l, --local         use "--local" option to overlay a built tree into the container
      -c, --cleanUp       cleans the test state (default: False)
          --app-config    APP_CONFIG (default:/netstore/datasets/customer4/simulationRig/appConfig)
      -S  --snapshotPath  SNAPSHOT_PATH
      -B  --baseDataPath  BASE_DATA_PATH
      -D  --deltasOnly    DELTAS_ONLY (default: False)

   [example]
      1. Install/uninstall containers:
          $0 -A install --local
          $0 -A install -i registry.int.xcalar.com/xcalar/xcalar:2.4.0-11064
          $0 -A uninstall

      2. start/ Stop containers:
          $0 -A start
          $0 -A stop

      3. Run data_gen:
          $0 -A run_data_gen
          $0 -A run_data_gen -H localhost -a 18443 -U admin -P admin -k kafka1:19092 -m 3

      4. Run test_rig: ( materialize -> incremenatl -> snapshot -> stop )
          $0 -A run_test_rig
          $0 -A run_test_rig -H localhost -a 18443 -U admin -P admin --snapshotPath /tmp --app-config /netstore/datasets/customer4/simulationRig/appConfig -p 2

      5. Stop test_rig:
          $0 -A stop_test_rig
          $0 -A stop_test_rig -H localhost  -a 18443 -U admin -P admin -p 2
EOF
}

get_last_imagetag() {
    echo $(dcc ls | grep 'xcalar/xcalar' | sed -e 's/^[[:space:]]*//' | head -1)
}

install_package() {
    docker exec xcalar-0 /opt/xcalar/bin/python3 -m pip install --no-cache-dir -t /mnt/xcalar/pysite --no-index --trusted-host netstore --find-links http://netstore/gslab/wheels -r /netstore/gslab/wheels/requirements.txt
    docker exec xcalar-0 chown -R xcalar:xcalar /mnt/xcalar/pysite
    docker exec xcalar-0 rm -rf /root/.cache /root/.pip
}

install_cluster() {
    echo "Launch xcalar ..."

    VOL_ARGS=()
    if [ -n "${DCC_VOLUMES:-}" ]; then
        local vol
        for vol in "${DCC_VOLUMES[@]}"; do
            VOL_ARGS+=(-v $vol)
        done
    fi
    echo "dcc run -i ${IMAGE_TAG} -n ${NODES} ${LOCAL} ${VOL_ARGS[@]}"
    dcc run -i ${IMAGE_TAG} -n ${NODES} ${LOCAL} ${VOL_ARGS[@]}
    echo "After launch xcalar ..."
    docker ps
}

install_kafka() {
    echo "Launch kafka ...${XLRDIR}"
    (cd $XLRDIR/docker/kafka && make up)
    echo "After launch kafka ..."
    docker ps
}

run_cluster() {
    echo "start xcalar cluster..."
    dcc start
}

run_kafka() {
    echo "start kafka ..."
    (cd $XLRDIR/docker/kafka && make up)
}

stop_cluster() {
    echo "Stop xcalar cluster..."
    dcc stop
}

stop_kafka() {
    echo "Stop kafka ..."
    (cd $XLRDIR/docker/kafka && make down)
}

save_cluster_logs() {
    # N.B.: this only works for local, containerized DCC
    if [ -z "${SAVE_LOGS}" ]; then
	return
    fi
    if [ ! -w "${SAVE_LOGS}" ]; then
	echo "${SAVE_LOGS} not a directory or not writable"
	return
    fi
    for ii in $(seq 1 $NODES); do
	node_name="xcalar-$(($ii-1))"
	node_root="${SAVE_LOGS}/$node_name"
	mkdir -p "$node_root"
	docker cp ${node_name}:/var/log/xcalar/. ${node_root} | true
    done
    docker cp xcalar-0:/mnt/xcalar/DataflowStatsHistory/ ${SAVE_LOGS} | true
}

delete_cluster() {
    echo "Uninstall xcalar cluster..."
    dcc rm -f
    echo "Uninstall kafka..."
    (cd $XLRDIR/docker/kafka && make down)
}

run_data_gen() {
    echo "generate base table and delta ..."
    baseDataPathArg=""
    if [ ! -z $BASE_DATA_PATH ]; then
        baseDataPathArg="-e ${BASE_DATA_PATH}"
    fi
    cmdPath="$XLRDIR/src/bin/tests/simulationRig/dataGen/data_gen.py"
    cmdArgs="${connectArgs} -k ${KAFKASERVER} -s ${SESSIONNAME} -b ${BASESIZE} -d ${DELTASIZE} -m ${NUMBATCHES} ${baseDataPathArg} ${CLEANUP} ${DELTAS_ONLY}"
    echo "python ${cmdPath} ${cmdArgs}"
    python ${cmdPath} ${cmdArgs}
}

run_test_rig_main() {
    if python -c 'import pkgutil; exit(not pkgutil.find_loader("dill"))'; then
        echo 'dill found'
    else
        echo 'pip install dill'
        pip install dill
    fi
    echo "load data lake stream ..."
    snapshotPathArg=""
    baseDataPathArg=""
    if [ ! -z $SNAPSHOT_PATH ]; then
        snapshotPathArg="--snapshotPath ${SNAPSHOT_PATH}"
    fi
    if [ ! -z $BASE_DATA_PATH ]; then
        baseDataPathArg="--baseDataPath ${BASE_DATA_PATH}"
    fi
    cmdPath="$XLRDIR/src/bin/tests/simulationRig/test_rig_main.py"
    cmdArgs="${connectArgs} --app-config ${APP_CONFIG} --num-premises ${PREMISES} ${baseDataPathArg} ${snapshotPathArg}"

    echo "python ${cmdPath} ${cmdArgs}"
    python ${cmdPath} ${cmdArgs}
}

stop_test_rig_main() {
    if python -c 'import pkgutil; exit(not pkgutil.find_loader("dill"))'; then
        echo 'dill found'
    else
        echo 'pip install dill'
        pip install dill
    fi
    echo "stop test_rig_main ..."
    snapshotPathArg=""
    baseDataPathArg=""
    if [ ! -z $SNAPSHOT_PATH ]; then
        snapshotPathArg="--snapshotPath ${SNAPSHOT_PATH}"
    fi
    if [ ! -z $BASE_DATA_PATH ]; then
        baseDataPathArg="--baseDataPath ${BASE_DATA_PATH}"
    fi
    cmdPath="$XLRDIR/src/bin/tests/simulationRig/test_rig_main.py"
    cmdArgs="${connectArgs} --app-config ${APP_CONFIG} --num-premises ${PREMISES} ${baseDataPathArg} ${snapshotPathArg} --action stop"
    echo "python ${cmdPath} ${cmdArgs}"
    python ${cmdPath} ${cmdArgs}
}

# ---------------
# main
# ---------------
ACTION=''
IMAGE_TAG=$(get_last_imagetag)
BUILD_ID=$(echo ${IMAGE_TAG} | cut -d '-' -f 2 | sed 's/.*-\(.*\)/\1/' | tr -d '\n')
if [[ -d /netstore/builds/byJob/BuildTrunk/${BUILD_ID} ]]; then
    SHA=$(head -1 /netstore/builds/byJob/BuildTrunk/${BUILD_ID}/BUILD_SHA | sed 's/.*(\(.*\))/\1/' | tr -d '\n')
else
    SHA=$(head -1 /netstore/builds/byJob/BuildCustom/${BUILD_ID}/BUILD_SHA | sed 's/.*(\(.*\))/\1/' | tr -d '\n')
fi

set -e
init

i=$(($# + 1)) # index of the first non-existing argument
declare -A longoptspec
longoptspec=(
    [ACTION]=1
    [IMAGE_TAG]=1
    [IMAGE_TAG]=1
    [NODES]=1
    [PREMISES]=1
    [XCALARHOST]=1
    [APIPORT]=1
    [XCALARUSER]=1
    [XCALARPASS]=1
    [SESSIONNAME]=1
    [BASESIZE]=1
    [DELTASIZE]=1
    [NUMBATCHES]=1
    [KAFKASERVER]=1
    [DCC_VOLUMES]=1
    [SAVE_LOGS]=1
    [LOCAL]=0
    [cleanUp]=0
    [deltasOnly]=0
    [appConfig]=1
    [snapshotPath]=1
    [baseDataPath]=1
)
optspec=":A:B:D:i:n:p:H:a:U:P:s:b:d:m:k:l:L:c:S:v:h-:"

while getopts "$optspec" opt; do
    while true; do
        case "${opt}" in
            -)                                    #OPTARG is name-of-long-option or name-of-long-option=value
                if [[ ${OPTARG} =~ .*=.* ]]; then # with this --key=value format only one argument is possible
                    opt=${OPTARG/=*/}
                    ((${#opt} <= 1)) && {
                        echo "Syntax error: Invalid long option '$opt'" >&2
                        exit 2
                    }
                    if (($((longoptspec[$opt])) != 1)); then
                        echo "Syntax error: Option '$opt' does not support this syntax." >&2
                        exit 2
                    fi
                    OPTARG=${OPTARG#*=}
                else #with this --key value1 value2 format multiple arguments are possible
                    opt="$OPTARG"
                    ((${#opt} <= 1)) && {
                        echo "Syntax error: Invalid long option '$opt'" >&2
                        exit 2
                    }
                    OPTARG=(${@:OPTIND:$((longoptspec[$opt]))})
                    ((OPTIND += longoptspec[$opt]))
                    echo $OPTIND
                    ((OPTIND > i)) && {
                        echo "Syntax error: Not all required arguments for option '$opt' are given." >&2
                        exit 3
                    }
                fi

                continue #now that opt/OPTARG are set we can process them as
                # if getopts would've given us long options
                ;;
            h | help)
                menu
                exit 0
                ;;
            A | ACTION)
                ACTION=${OPTARG[0]}
                ;;
            i | IMAGE_TAG)
                IMAGE_TAG=${OPTARG[0]}
                ;;
            i | IMAGE_TAG)
                IMAGE_TAG=${OPTARG[0]}
                ;;
            n | NODES)
                NODES=${OPTARG[0]}
                ;;
            p | PREMISES)
                PREMISES=${OPTARG[0]}
                ;;
            H | XCALARHOST)
                XCALARHOST=${OPTARG[0]}
                ;;
            a | APIPORT)
                APIPORT=${OPTARG[0]}
                ;;
            U | XCALARUSER)
                XCALARUSER=${OPTARG[0]}
                ;;
            P | XCALARPASS)
                XCALARPASS=${OPTARG[0]}
                ;;
            s | SESSIONNAME)
                SESSIONNAME=${OPTARG[0]}
                ;;
            b | BASESIZE)
                BASESIZE=${OPTARG[0]}
                ;;
            d | DELTASIZE)
                DELTASIZE=${OPTARG[0]}
                ;;
            D | deltasOnly)
                DELTAS_ONLY='--deltasOnly'
                ;;
            m | NUMBATCHES)
                NUMBATCHES=${OPTARG[0]}
                ;;
            k | KAFKASERVER)
                KAFKASERVER=${OPTARG[0]}
                ;;
            l | local)
                LOCAL='--local'
                ;;
            c | cleanUp)
                CLEANUP='-c'
                ;;
            c | cleanUp)
                CLEANUP='-c'
                ;;
            appConfig)
                APP_CONFIG=${OPTARG[0]}
                ;;
            B | baseDataPath)
                BASE_DATA_PATH=${OPTARG[0]}
                ;;
            S | snapshotPath)
                SNAPSHOT_PATH=${OPTARG[0]}
                ;;
            v | DCC_VOLUMES)
                DCC_VOLUMES+=(${OPTARG[0]})
                ;;
            L | SAVE_LOGS)
                SAVE_LOGS=${OPTARG[0]}
                ;;
            ?)
                echo "Syntax error: Unknown short option '$OPTARG'" >&2
                exit 2
                ;;
            *)
                echo "Syntax error: Unknown long option '$opt'" >&2
                exit 2
                ;;
        esac
        break
    done
done

connectArgs="-H ${XCALARHOST} -a ${APIPORT} -U ${XCALARUSER} -P ${XCALARPASS}"

case "$ACTION" in
    install)
        install_cluster
        install_kafka
        install_package
        ;;
    uninstall)
	save_cluster_logs
        delete_cluster
        ;;
    start)
        run_cluster
        run_kafka
        echo 'start xcalar cluster ...'
        sleep 15
        ;;
    stop)
        stop_cluster
        stop_kafka
        ;;
    run_data_gen)
        run_data_gen
        ;;
    run_test_rig)
        echo "run test_rig ..."
        run_test_rig_main
        ;;
    stop_test_rig)
        echo "stop test_rig ..."
        stop_test_rig_main
        ;;
    *)
        menu
        exit 0
        ;;
esac

exit $?
# End of file
