

xc_protobuf_generate(
    CPP_HDRS
        protoHs
    CPP_SRCS
        protoCpps
    PY_SRCS
        protoPys
    JS_OUT
        "${CMAKE_CURRENT_BINARY_DIR}/js"
    JS_SRCS
        protoJss
    PROTO_PATH
        "${PROTO_DST_ROOT}"
    FILES
        ${LinkedProto}
        ${PROTO_SERVICE_FILES}
    DEPENDS
        PROTO_SERVICE_DST
        ${LinkedProto}
    )

xc_protobuf_generate(
    CPP_HDRS
        durableProtoHs
    CPP_SRCS
        durableProtoCpps
    PY_SRCS
        durableProtoPys
    PROTO_PATH
        "${PROTO_SRC_ROOT}/durable"
    FILES
        "${PROTO_SRC_ROOT}/durable/DurableObject.proto"
        "${PROTO_SRC_ROOT}/durable/subsys/DurableSession.proto"
        "${PROTO_SRC_ROOT}/durable/subsys/DurableDag.proto"
        "${PROTO_SRC_ROOT}/durable/subsys/DurableKvStore.proto"
        "${PROTO_SRC_ROOT}/durable/subsys/DurableDataTarget.proto"
        "${PROTO_SRC_ROOT}/durable/subsys/versions/DurableSessionV2.proto"
        "${PROTO_SRC_ROOT}/durable/subsys/versions/DurableDagV1.proto"
        "${PROTO_SRC_ROOT}/durable/subsys/versions/DurableKvStoreV1.proto"
        "${PROTO_SRC_ROOT}/durable/subsys/versions/DurableDataTargetV1.proto"
    DEPENDS
        LINKED_BINARIES
    )

# Generate C file containing GIT SHA version info for .proto files
set(VER_SHA_HEADER "${CMAKE_CURRENT_BINARY_DIR}/DurableVersions.h")
add_custom_command(
    OUTPUT ${VER_SHA_HEADER}
    COMMAND ${GENIDLVERS} "${CMAKE_SOURCE_DIR}/xcalar-idl/" > ${VER_SHA_HEADER}
    DEPENDS ${durableProtoCpps} ${durableProtoHs})

add_custom_target(GENERATED_HEADERS_PROTOBUF DEPENDS ${protoHs} ${durableProtoHs} ${VER_SHA_HEADER})
add_custom_target(GENERATED_PYTHON_PROTOBUF DEPENDS ${protoPys} ${durableProtoPys})
add_custom_target(GENERATED_JAVASCRIPT_PROTOBUF DEPENDS js.stamp ${protoJss})
add_custom_command(
    OUTPUT js.stamp
    COMMAND touch js.stamp
    DEPENDS ${protoJss})

add_library(libxprotobuf STATIC
            ${protoCpps} ${protoHs}
            ${durableProtoCpps} ${durableProtoHs}
            ${VER_SHA_HEADER})

target_link_libraries(libxprotobuf
    PRIVATE ${PROTOBUF_LIBRARIES})
