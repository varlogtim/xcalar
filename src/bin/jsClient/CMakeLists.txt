include(genProto)

set(OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/jsPackage")
set(SERVER_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/jsPackage/serviceInfo")
set(PROTO_JS_DIR "${CMAKE_BINARY_DIR}/src/lib/libprotobuf/js")

set(STATIC_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/Client.js"
    "${CMAKE_CURRENT_SOURCE_DIR}/index.js"
    "${CMAKE_CURRENT_SOURCE_DIR}/sample.js"
    "${CMAKE_CURRENT_SOURCE_DIR}/sample-node.js"
    "${CMAKE_CURRENT_SOURCE_DIR}/package.json"
    )

xc_protobuf_generate(
    JS_SERVICE_CLIENT_SRCS
        jsServiceClientSrcs
    JS_SERVICE_SERVER_SRCS
        jsServiceInfoSrcs
    PROTO_PATH
        "${PROTO_DST_ROOT}"
    FILES
        ${PROTO_SERVICE_FILES}
    )

add_custom_command(
    OUTPUT
        package.stamp
    COMMAND
        ${CMAKE_COMMAND} -E make_directory ${OUT_DIR}
    COMMAND
        ${CMAKE_COMMAND} -E copy_directory ${PROTO_JS_DIR} ${OUT_DIR}
    COMMAND
        ${CMAKE_COMMAND} -E copy
            ${STATIC_FILES}
            ${OUT_DIR}
    COMMAND
        ${CMAKE_COMMAND} -E copy
            ${jsServiceClientSrcs}
            ${OUT_DIR}
    COMMAND
        ${CMAKE_COMMAND} -E make_directory ${SERVER_OUT_DIR}
    COMMAND
        ${CMAKE_COMMAND} -E copy
            ${jsServiceInfoSrcs}
            ${SERVER_OUT_DIR}
    COMMAND
        touch package.stamp
    DEPENDS
        GENERATED_JAVASCRIPT_PROTOBUF
        "${PROTO_JS_DIR}/../js.stamp"
        ${STATIC_FILES}
        ${jsServiceClientSrcs}
        ${jsServiceInfoSrcs}
    COMMENT
        "Building js client library"
    )

add_custom_target(jsClient DEPENDS package.stamp)
add_dependencies(xceBins jsClient)
