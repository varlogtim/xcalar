set(GENERATED_FILES
    ${CMAKE_CURRENT_BINARY_DIR}/StatusCode.h
    ${CMAKE_CURRENT_BINARY_DIR}/Status.thrift
    ${CMAKE_CURRENT_BINARY_DIR}/LogMessages.h
    ${CMAKE_CURRENT_BINARY_DIR}/LogLevel.h
    ${CMAKE_CURRENT_BINARY_DIR}/Subsys.h)

set(logSourceFile "${CMAKE_SOURCE_DIR}/src/data/lang/${PKGLANG}/LogMessages.h")
set(statusEnum "${CMAKE_SOURCE_DIR}/src/data/lang/${PKGLANG}/StatusCode.enum")
set(subsysEnum "${CMAKE_SOURCE_DIR}/src/data/lang/${PKGLANG}/Subsys.enum")
set(logLevelEnum "${CMAKE_SOURCE_DIR}/src/include/LogLevel.enum")

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/StatusCode.h
    COMMAND ${GENENUM} -i ${statusEnum} -r "StatusCode.h"
    DEPENDS ${statusEnum})

# XXX we call this Status instead of StatusCode for thrift
add_custom_command(
    OUTPUT "Status.thrift"
    COMMAND ${GENENUM} -i ${statusEnum} -t "Status.thrift"
    DEPENDS ${statusEnum})

add_custom_command(
    OUTPUT "LogMessages.h"
    COMMAND ${CMAKE_COMMAND} -E create_symlink
        ${logSourceFile} "LogMessages.h"
    DEPENDS ${logSourceFile})

add_custom_command(
    OUTPUT "Subsys.h"
    COMMAND ${GENENUM} -i ${subsysEnum} -r "Subsys.h"
    DEPENDS ${subsysEnum})

add_custom_command(
    OUTPUT "LogLevel.h"
    COMMAND ${GENENUM} -i ${logLevelEnum} -r "LogLevel.h"
    DEPENDS ${logLevelEnum})

add_custom_target(GENERATED_HEADERS_PRIMITIVES
    DEPENDS ${GENERATED_FILES})
