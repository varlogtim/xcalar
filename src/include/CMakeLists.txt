set(LINKED_FILES
    ${CMAKE_SOURCE_DIR}/src/include/${OS}/${ASPACE}/SocketTypes.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/JsonGenEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/DataFormatEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/SourceTypeEnum.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/DataTargetEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/JoinOpEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/UnionOpEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/XcalarEvalEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/DagRefTypeEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/QueryStateEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/FunctionCategory.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/DagStateEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/OrderingEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/UdfTypeEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/QueryParserEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/CsvLoadArgsEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LicenseTypes.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/RuntimeEnums.h
    ${CMAKE_BINARY_DIR}/src/lib/libenum/DataflowEnums.h
    # Thrift files
    ${CMAKE_BINARY_DIR}/src/lib/libenum/DataFormatEnums.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/SourceTypeEnum.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/DataTargetEnums.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/JoinOpEnums.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/UnionOpEnums.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/XcalarEvalEnums.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/DagRefTypeEnums.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/QueryStateEnums.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/FunctionCategory.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/DagStateEnums.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/OrderingEnums.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/CsvLoadArgsEnums.thrift
    ${CMAKE_BINARY_DIR}/src/lib/libenum/QueryParserEnums.thrift)

set(LOCAL_HEADERS "")
foreach(headerSource ${LINKED_FILES})
    get_filename_component(localFile ${headerSource} NAME)
    add_custom_command(
        OUTPUT ${localFile}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${headerSource} ${localFile}
        DEPENDS ${headerSource})
    list(APPEND LOCAL_HEADERS ${localFile})
endforeach(headerSource)

# Clang-format all headers
add_custom_target(clangFormatHeaders
    COMMAND
        find ${CMAKE_CURRENT_SOURCE_DIR} -name "*.h" | xargs -n1 -P0 clang-format -i
    VERBATIM
    COMMENT
        "formatting header files")
add_dependencies(clangFormat clangFormatHeaders)

# Clang-format all headers
add_custom_target(clangFormatVerifyHeaders
    COMMAND
        find ${CMAKE_CURRENT_SOURCE_DIR} -name "*.h" | xargs -I {} -n1 -P0 bash -c "clang-format {} | diff -q {} /dev/fd/0"
    VERBATIM
    COMMENT
        "formatting header files")
add_dependencies(clangFormatVerify clangFormatVerifyHeaders)

# Generate LicenseEmbedded.h
set(licenseHeader "LicenseEmbedded.h")
add_custom_command(
    OUTPUT ${licenseHeader}
    COMMAND ${GENEMBEDDED} -p `cat ${PASSWORDFILE}` -k "${PUBKEYFILE}" -r "${licenseHeader}"
    DEPENDS ${PASSWORDFILE} ${PUBKEYFILE} ${GENEMBEDDED}
        )
list(APPEND LOCAL_HEADERS ${licenseHeader})

add_custom_target(GENERATED_HEADERS_INCLUDE
    DEPENDS ${LOCAL_HEADERS} GENERATED_HEADERS_ENUMS GENERATED_PYTHON_ENUMS)

add_subdirectory(primitives)
add_subdirectory(libapis)
add_subdirectory(hash)
add_subdirectory(test)

# This is a target representing all of the headers. This should be built
# before any source files, since they might include them
add_custom_target(GENERATED_HEADERS
    DEPENDS
        GENERATED_HEADERS_INCLUDE
        GENERATED_HEADERS_PRIMITIVES
        GENERATED_HEADERS_LIBAPIS
        GENERATED_HEADERS_HASH
        GENERATED_HEADERS_FUNCTEST
        GENERATED_HEADERS_PROTOBUF
        GENERATED_HEADERS_XCRPC
)
