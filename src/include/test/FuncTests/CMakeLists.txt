set(LINKED_HEADERS
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibMsgFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibXdbFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibBcFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibOperatorsFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibKvStoreFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibDsFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibQueryParserFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibQueryEvalFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibOptimizerFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibDfFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibLogFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibSessionFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibQmFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/ChildFunFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibDagFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibRuntimeFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibAppFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibNsFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibTableFuncTestConfig.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibDemystifyFuncTestConfig.h)

set(LOCAL_HEADERS "")
foreach(headerSource ${LINKED_HEADERS})
    get_filename_component(localFile ${headerSource} NAME)
    add_custom_command(
        OUTPUT ${localFile}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${headerSource} ${localFile}
        DEPENDS ${headerSource})
    list(APPEND LOCAL_HEADERS ${localFile})
endforeach(headerSource)

add_custom_target(GENERATED_HEADERS_FUNCTEST
    DEPENDS ${LOCAL_HEADERS} GENERATED_HEADERS_ENUMS)
