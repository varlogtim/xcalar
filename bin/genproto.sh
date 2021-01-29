#!/bin/bash
set -e

genProto() {
    SRC_ROOT="$1"
    IDL_INPUT_PATH="$2"

    declare -a protoCmds=(

    "genproto.py -x ${SRC_ROOT} -n dag \
        -P XcalarApiInput,SerializedHdr,DagNodeHdr \
        -s ::Dag::SerializedDagNodesContainer -f ${SRC_ROOT}/src/include/dag/Dag.h \
        -p ${SRC_ROOT}/src/include/pb/durable/subsys/versions/DurableDagV1 \
        -c ${SRC_ROOT}/src/lib/libdag/DagDurable"

    "genproto.py -x ${SRC_ROOT} -n kvstore \
        -P KvStorePersistedData \
        -s ::KvStore::KvStorePersistedData -f ${SRC_ROOT}/src/include/kvstore/KvStoreTypes.h \
        -p ${SRC_ROOT}/src/include/pb/durable/subsys/versions/DurableKvStoreV1 \
        -c ${SRC_ROOT}/src/lib/libkvstore/KvStoreDurable"

    "genproto.py -x ${SRC_ROOT} -n datatarget \
        -P TargetListPersistedData \
        -s ::DataTargetManager::TargetListPersistedData -f ${SRC_ROOT}/src/include/export/DataTarget.h \
        -p ${SRC_ROOT}/src/include/pb/durable/subsys/versions/DurableDataTargetV1 \
        -c ${SRC_ROOT}/src/lib/libexport/DataTargetDurable"

    "genproto.py -x ${SRC_ROOT} -n session \
        -P DurableSessionV2 \
        -s ::SessionMgr::DurableSessionV2 -f ${SRC_ROOT}/src/include/session/Sessions.h \
        -p ${SRC_ROOT}/src/include/pb/durable/subsys/versions/DurableSessionV2 \
        -c ${SRC_ROOT}/src/lib/libsession/SessionDurable"

    )

    echo "Verifying IDLs and SerDes methods..."

    for cmd in "${protoCmds[@]}"
    do
        if [ -n "${IDL_INPUT_PATH}" ]
        then
            durable="$(echo $cmd | sed 's/.*durable//' | sed 's/-c.*//')"
            cmd+=" -i ${IDL_INPUT_PATH}${durable}"
        fi

        if [ -d "${BUILD_DIR}" ]
        then
            cmd+=" -C ${BUILD_DIR}"
        fi

        cmd=$(echo "$cmd" | tr -s " ")

        if [ -n "$VERBOSE" ]
        then
            echo "genproto cmd: $cmd"
        fi

        eval "$cmd"
    done
}
