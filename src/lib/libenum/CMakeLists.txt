# Add all the files here that may be required to deem Version compatibility
# between XD and XCE.
# XXX LibApisCommon.h file seems unnecessary, but it's best to be conservative,
# since LibApisCommon.h and LibApisCommon.thrift are kept compatible
# manually now.
set(VERSIONED_API_SOURCE
    ${CMAKE_SOURCE_DIR}/src/include/libapis/LibApisCommon.h
    ${CMAKE_SOURCE_DIR}/src/include/libapis/LibApisCommon.thrift
    ${CMAKE_SOURCE_DIR}/src/include/UdfTypeEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/SourceTypeEnum.enum
    ${CMAKE_SOURCE_DIR}/src/include/OrderingEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/DataFormatEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/JsonGenEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/JoinOpEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/UnionOpEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/XcalarEvalEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/DagStateEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/DagRefTypeEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/QueryParserEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/libapis/LibApisEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/libapis/LibApisConstants.enum
    ${CMAKE_SOURCE_DIR}/src/include/QueryStateEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/DataTargetEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/CsvLoadArgsEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/license/LicenseTypes.enum
    ${CMAKE_SOURCE_DIR}/src/data/lang/${PKGLANG}/Subsys.enum
    ${CMAKE_SOURCE_DIR}/src/data/lang/${PKGLANG}/StatusCode.enum
    ${CMAKE_SOURCE_DIR}/src/data/lang/${PKGLANG}/FunctionCategory.enum
    ${CMAKE_SOURCE_DIR}/src/include/runtime/RuntimeEnums.enum
    ${CMAKE_SOURCE_DIR}/src/include/LogLevel.enum
    ${CMAKE_SOURCE_DIR}/src/include/querymanager/DataflowEnums.enum
)

# All the Xcrpc interfaces are in the below path. These are all required to
# deem Version compatibility between XD and XCE.
set(PROTO_DIR "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/")

include_directories(${CMAKE_CURRENT_BINARY_DIR})
set(ENUM_CPP_FILES "")
set(ENUM_H_FILES "")
set(ENUM_THRIFT_FILES "")
set(ENUM_JSON_FILES "")

foreach(enumFile ${ENUM_FILES})
    # Get our raw enum name
    get_filename_component(enumName ${enumFile} NAME_WE)
    set(cppFile "${enumName}.cpp")
    set(hFile "${enumName}.h")
    set(thriftFile "${enumName}.thrift")
    set(jsonDir "JSONENUMLIB")
    set(pythonFile "${enumName}.py")
    set(pythonDir "PYTHONENUMLIB")
    set(prefixArg "")

    # we need to handle StatusCode.thrift specially because we don't want it
    # to be called StatusCode
    # we also want the C++ to log error codes
    if(${enumName} STREQUAL "StatusCode")
        set(thriftFile "Status.thrift")
        set(enumName "Status")
        # The prefixArg must be a semi-colon delimited list which
        # will be expanded properly into "-e XCE". Using a space
        # causes the space to be escaped after expansion ("-e\ XCE")
        # which genenum.py will use to prefix the string.
        set(prefixArg "-e;XCE")
    endif()

    # Make the .cpp, .h, .thrit .proto .json files
    add_custom_command(
        OUTPUT "${cppFile}"
        COMMAND ${GENENUM} -i ${enumFile} -c "${cppFile}" ${prefixArg}
        DEPENDS ${enumFile} ${GENENUM}
        )
    add_custom_command(
        OUTPUT "${hFile}"
        COMMAND ${GENENUM} -i ${enumFile} -r "${hFile}"
        DEPENDS ${enumFile} ${GENENUM}
        )
    add_custom_command(
        OUTPUT "${thriftFile}"
        COMMAND ${GENENUM} -i ${enumFile} -t "${thriftFile}"
        DEPENDS ${enumFile} ${GENENUM}
        )
    add_custom_command(
        OUTPUT "${jsonDir}/${enumName}ToInt.json" "${jsonDir}/${enumName}ToStr.json" "${jsonDir}/${enumName}FromStr.json"
        COMMAND ${GENENUM} -i ${enumFile} -j "${jsonDir}"
        DEPENDS ${enumFile} ${GENENUM}
        )
    add_custom_command(
        OUTPUT "${pythonDir}/${pythonFile}"
        COMMAND ${GENENUM} -i ${enumFile} -y "${pythonDir}/${pythonFile}"
        DEPENDS ${enumFile} ${GENENUM}
    )

    list(APPEND ENUM_CPP_FILES "${CMAKE_CURRENT_BINARY_DIR}/${cppFile}")
    list(APPEND ENUM_H_FILES "${CMAKE_CURRENT_BINARY_DIR}/${hFile}")
    list(APPEND ENUM_THRIFT_FILES "${CMAKE_CURRENT_BINARY_DIR}/${thriftFile}")
    list(APPEND ENUM_JSON_FILES "${CMAKE_CURRENT_BINARY_DIR}/${jsonDir}/${enumName}ToInt.json")
    list(APPEND ENUM_JSON_FILES "${CMAKE_CURRENT_BINARY_DIR}/${jsonDir}/${enumName}ToStr.json")
    list(APPEND ENUM_JSON_FILES "${CMAKE_CURRENT_BINARY_DIR}/${jsonDir}/${enumName}FromStr.json")
    list(APPEND ENUM_PYTHON_FILES "${CMAKE_CURRENT_BINARY_DIR}/${pythonDir}/${pythonFile}")
endforeach(enumFile)


add_custom_command(
    OUTPUT ${VERSION_ENUM}
    COMMAND ${GENVERSION} -i '${VERSIONED_API_SOURCE}' -o ${VERSION_ENUM}
    DEPENDS ${VERSIONED_API_SOURCE} ${GENVERSION})

add_custom_command(
    OUTPUT ${PROTO_VERSION_ENUM}
    COMMAND ${GENPROTOVERSION} -d ${PROTO_DIR} -o ${PROTO_VERSION_ENUM}
    DEPENDS ${PROTO_DIR} ${GENPROTOVERSION})

add_custom_target(GENERATED_HEADERS_ENUMS
    DEPENDS ${ENUM_H_FILES} ${ENUM_THRIFT_FILES}  ${ENUM_JSON_FILES} ${ENUM_PYTHON_FILES})

add_xce_library(libenum STATIC ${ENUM_CPP_FILES} ${ENUM_H_FILES})
