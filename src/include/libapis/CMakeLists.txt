set(LINKED_FILES
    ${CMAKE_BINARY_DIR}/src/lib/libenum/XcalarApiVersionSignature.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/XcrpcApiVersionSignature.h

    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibApisConstants.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibApisEnums.h
    # Thrift files
    ${CMAKE_BINARY_DIR}/src/lib/libenum/XcalarApiVersionSignature.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibApisConstants.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LibApisEnums.thrift)

set(VERSION_ENUM "XcalarApiVersionSignature.enum")

set(LOCAL_HEADERS "")
foreach(headerSource ${LINKED_FILES})
    get_filename_component(localFile ${headerSource} NAME)
    add_custom_command(
        OUTPUT ${localFile}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${headerSource} ${localFile}
        DEPENDS GENERATED_HEADERS_ENUMS)
    list(APPEND LOCAL_HEADERS ${localFile})
endforeach(headerSource)

add_custom_target(GENERATED_HEADERS_LIBAPIS
    DEPENDS ${LOCAL_HEADERS})
