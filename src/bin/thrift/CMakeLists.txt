find_package(Thrift REQUIRED)
include_directories(
    "${Thrift_INCLUDE_DIRS}"
)
set(THRIFTC thrift)
set(THRIFT_FILES
    "${CMAKE_BINARY_DIR}/src/include/primitives/Status.thrift"
    "${CMAKE_BINARY_DIR}/src/include/DataFormatEnums.thrift"
    "${CMAKE_BINARY_DIR}/src/include/DataTargetEnums.thrift"
    "${CMAKE_SOURCE_DIR}/src/include/DataTargetTypes.thrift"
    "${CMAKE_SOURCE_DIR}/src/include/DagTypes.thrift"
    "${CMAKE_SOURCE_DIR}/src/include/UdfTypes.thrift"
    "${CMAKE_BINARY_DIR}/src/include/QueryStateEnums.thrift"
    "${CMAKE_BINARY_DIR}/src/include/SourceTypeEnum.thrift"
    "${CMAKE_BINARY_DIR}/src/include/DagStateEnums.thrift"
    "${CMAKE_BINARY_DIR}/src/include/DagRefTypeEnums.thrift"
    "${CMAKE_BINARY_DIR}/src/include/OrderingEnums.thrift"
    "${CMAKE_BINARY_DIR}/src/include/JoinOpEnums.thrift"
    "${CMAKE_BINARY_DIR}/src/include/UnionOpEnums.thrift"
    "${CMAKE_BINARY_DIR}/src/include/CsvLoadArgsEnums.thrift"
    "${CMAKE_BINARY_DIR}/src/include/XcalarEvalEnums.thrift"
    "${CMAKE_BINARY_DIR}/src/include/libapis/LibApisEnums.thrift"
    "${CMAKE_BINARY_DIR}/src/include/libapis/LibApisConstants.thrift"
    "${CMAKE_BINARY_DIR}/src/include/libapis/XcalarApiVersionSignature.thrift"
    "${CMAKE_BINARY_DIR}/src/include/FunctionCategory.thrift"
    "${CMAKE_SOURCE_DIR}/src/include/libapis/LibApisCommon.thrift")

# This file includes all other thrift files; meaning it will generate them all
set(TOP_THRIFT_FILE "LibApisCommon.thrift")

add_definitions(-D_THRIFT_USING_CLANG_LIBCXX=1 -D_THRIFT_USING_MICROSOFT_STDLIB=0 -DT_GLOBAL_DEBUG_VIRTUAL=0)
add_definitions(-DUSE_STD_THREAD=1 -DUSE_BOOST_THREAD=0)


set(SKEL_DIR "${CMAKE_CURRENT_BINARY_DIR}/cppSkeleton")
set(GEN_DIRS ${SKEL_DIR})

set(THRIFT_LOCAL_FILES "")
set(THRIFT_CPP_FILES "")
set(THRIFT_H_FILES "")

foreach(thriftSource ${THRIFT_FILES})
    # Get the thrift name like 'LibApisCommon'
    get_filename_component(thriftName ${thriftSource} NAME_WE)

    add_custom_command(
        OUTPUT "${thriftName}.thrift"
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${thriftSource} "${thriftName}.thrift"
        DEPENDS ${thriftSource} GENERATED_HEADERS)

    list(APPEND THRIFT_LOCAL_FILES "${CMAKE_CURRENT_BINARY_DIR}/${thriftName}.thrift")

    list(APPEND THRIFT_CPP_FILES "${SKEL_DIR}/${thriftName}_constants.cpp")
    list(APPEND THRIFT_CPP_FILES "${SKEL_DIR}/${thriftName}_types.cpp")

    list(APPEND THRIFT_H_FILES "${SKEL_DIR}/${thriftName}_constants.h")
    list(APPEND THRIFT_H_FILES "${SKEL_DIR}/${thriftName}_types.h")
endforeach(thriftSource)

list(APPEND THRIFT_H_FILES "${SKEL_DIR}/XcalarApiService.h")
list(APPEND THRIFT_CPP_FILES "${SKEL_DIR}/XcalarApiService.cpp")

# Create output directories
foreach(genDir ${GEN_DIRS})
    add_custom_command(
        OUTPUT ${genDir}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${genDir})
endforeach(genDir)

# Generate python output
add_custom_command(
    OUTPUT
        py.stamp
    COMMAND
        ${CMAKE_COMMAND} -E make_directory py
    COMMAND
        ${THRIFTC} -r -out py -I ${CMAKE_CURRENT_BINARY_DIR} --allow-64bit-consts --gen py ${TOP_THRIFT_FILE}
    COMMAND
        ${CMAKE_COMMAND} -E copy "${CMAKE_CURRENT_SOURCE_DIR}/pyInit.py" py/__init__.py
    COMMAND
        touch py.stamp
    DEPENDS ${THRIFT_LOCAL_FILES})

add_custom_target(thriftPyFiles
    DEPENDS py.stamp)

# Generate javascript output
add_custom_command(
    OUTPUT
        js.stamp
    COMMAND
        ${CMAKE_COMMAND} -E make_directory js
    COMMAND
        ${THRIFTC} -r -out js -I ${CMAKE_CURRENT_BINARY_DIR} --allow-64bit-consts --gen js ${TOP_THRIFT_FILE}
    COMMAND
        touch js.stamp
    DEPENDS ${THRIFT_LOCAL_FILES})

add_custom_target(thriftJsFiles ALL
    DEPENDS js.stamp)

# Generate CPP static lib
add_custom_command(
    OUTPUT ${THRIFT_CPP_FILES} ${THRIFT_H_FILES} "${SKEL_DIR}/XcalarApiService_server.skeleton.cpp"
    COMMAND
        ${THRIFTC} -r -out ${SKEL_DIR} -I ${CMAKE_CURRENT_BINARY_DIR} --allow-64bit-consts --gen cpp ${TOP_THRIFT_FILE}
    DEPENDS ${THRIFT_LOCAL_FILES} ${SKEL_DIR})

add_xce_library(libthriftskel STATIC ${THRIFT_CPP_FILES} ${THRIFT_H_FILES})
# Thrift generates some wonky massive stack allocations in the _constants.cpp
# files. Check out StatusCode_constants.cpp: StatusCodeConstants::StatusCodeConstants
target_compile_options(libthriftskel PRIVATE -Wno-frame-larger-than=)
