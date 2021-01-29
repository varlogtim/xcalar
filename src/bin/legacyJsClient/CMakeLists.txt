# Old API layer js client
set(OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/jsPackage")
set(THRIFT_JS_DIR "${CMAKE_BINARY_DIR}/src/bin/thrift/js")

add_custom_command(
    OUTPUT
        package.stamp ${OUT_DIR}/XcalarApi.js ${OUT_DIR}/XcalarApiServiceAsync.js
    COMMAND
        ${CMAKE_COMMAND} -E make_directory ${OUT_DIR}
    COMMAND
        ${CMAKE_COMMAND} -E copy_directory ${THRIFT_JS_DIR} ${OUT_DIR}
    COMMAND
        ${CMAKE_COMMAND} -E copy
            ${CMAKE_CURRENT_SOURCE_DIR}/XcalarApi.js
            ${OUT_DIR}/XcalarApi.js
    COMMAND
        ${CMAKE_COMMAND} -E copy
            ${CMAKE_CURRENT_SOURCE_DIR}/XcalarApiServiceAsync.js
            ${OUT_DIR}/XcalarApiServiceAsync.js
    COMMAND
        touch package.stamp
    DEPENDS
        ${CMAKE_BINARY_DIR}/src/bin/thrift/js.stamp
        ${CMAKE_CURRENT_SOURCE_DIR}/XcalarApi.js
        ${CMAKE_CURRENT_SOURCE_DIR}/XcalarApiServiceAsync.js
    )

add_custom_target(legacyJsClient DEPENDS package.stamp)
add_dependencies(xceBins legacyJsClient)
