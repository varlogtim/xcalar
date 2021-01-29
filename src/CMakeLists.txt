# Build-specific options pertaining to code generation.
option(ENABLE_ASSERTIONS "Enable assertions" OFF)
option(ENABLE_FUNC_TESTS "Enable functional tests" ON)
option(BUFCACHEPOISON "Poison Buffer Cache allocations" OFF)
option(BUFCACHETRACE "Trace Buffer Cache allocations" OFF)
option(MEMTRACK "Track memAlloc/new stacks (only in debug)" OFF)
option(MEMTRACKTRACE "Trace memAlloc/new stacks (only in debug)" OFF)
option(CUSTOM_HEAP "Enable a custom heap implementation" OFF)
option(COVERAGE "Track code coverage" OFF)
option(INLINES "Enable inlining of functions" ON)
option(PROF "Enable code profiling tools" OFF)
option(XLR_VALGRIND "Enable advanced valgrind support" OFF)
option(USE_CCACHE "Allow ccache to speedup builds" ON)
option(ADDRESS_SANITIZER "Address Sanitizer" OFF)
option(MEMORY_SANITIZER "Memory Sanitizer" OFF)
option(THREAD_SANITIZER "Thread Sanitizer" OFF)
option(UB_SANITIZER "Undefined behavior Sanitizer" OFF)
option(BUFCACHESHADOW "BUFCACHESHADOW support" OFF)
option(BUFCACHESHADOWRETIRED "BUFCACHESHADOWRETIRED support" OFF)
option(XDBPAGEHDRMAP "XDBPAGEHDRMAP support" OFF)
option(MEMORY_PILE_PROTECT "Add memory pile guard" OFF)
option(MEMORY_PILE_POISON "Add memory pile poison" OFF)
option(TUPLE_BUFFER_POISON "Add tuple buffer poison" OFF)


# Usability options.
option(FORCE_COLORED_OUTPUT "Always produce ANSI-colored output when compiling." FALSE)

# Build-specific strings passed by the cmBuild driver.
set(XCALAR_BUILD_TYPE "undefined" CACHE STRING "XCALAR_BUILD_TYPE to be baked into the executable.")
add_definitions(-DXCALAR_BUILD_TYPE=${XCALAR_BUILD_TYPE})

message("COMPILE_OPTION: ENABLE_ASSERTIONS support is ${ENABLE_ASSERTIONS}")
if(ENABLE_ASSERTIONS)
    add_definitions(-DENABLE_ASSERTIONS)
endif()

message("COMPILE_OPTION: ENABLE_FUNC_TESTS support is ${ENABLE_FUNC_TESTS}")
if(NOT ENABLE_FUNC_TESTS)
    add_definitions(-DDISABLE_FUNC_TESTS)
endif()

message("COMPILE_OPTION: BUFCACHEPOISON support is ${BUFCACHEPOISON}")
if(BUFCACHEPOISON)
    add_definitions(-DBUFCACHEPOISON)
endif()

message("COMPILE_OPTION: BUFCACHETRACE support is ${BUFCACHETRACE}")
if(BUFCACHETRACE)
    add_definitions(-DBUFCACHETRACE)
endif()

message("COMPILE_OPTION: BUFCACHESHADOW support is ${BUFCACHESHADOW}")
if(BUFCACHESHADOW)
    add_definitions(-DBUFCACHESHADOW)
endif()

message("COMPILE_OPTION: BUFCACHESHADOWRETIRED support is ${BUFCACHESHADOWRETIRED}")
if(BUFCACHESHADOWRETIRED)
    add_definitions(-DBUFCACHESHADOWRETIRED)
endif()

message("COMPILE_OPTION: MEMORY_PILE_PROTECT support is ${MEMORY_PILE_PROTECT}")
if(MEMORY_PILE_PROTECT)
    add_definitions(-DMEMORY_PILE_PROTECT)
endif()

message("COMPILE_OPTION: MEMORY_PILE_POISON support is ${MEMORY_PILE_POISON}")
if(MEMORY_PILE_POISON)
    add_definitions(-DMEMORY_PILE_POISON)
endif()

message("COMPILE_OPTION: TUPLE_BUFFER_POISON support is ${TUPLE_BUFFER_POISON}")
if(TUPLE_BUFFER_POISON)
    add_definitions(-DTUPLE_BUFFER_POISON)
endif()

message("COMPILE_OPTION: XDBPAGEHDRMAP support is ${XDBPAGEHDRMAP}")
if(XDBPAGEHDRMAP)
    add_definitions(-DXDBPAGEHDRMAP)
endif()

message("COMPILE_OPTION: MEMTRACK support is ${MEMTRACK}")
if(MEMTRACK)
    add_definitions(-DMEMORY_TRACKING)
endif()

message("COMPILE_OPTION: MEMTRACKTRACE support is ${MEMTRACKTRACE}")
if(MEMTRACKTRACE)
    add_definitions(-DMEMTRACKTRACE)
endif()

message("COMPILE_OPTION: CUSTOM_HEAP support is ${CUSTOM_HEAP}")
if(CUSTOM_HEAP)
    add_definitions(-DCUSTOM_HEAP)
    set(CUSTOM_HEAP_LIBRARIES :libjemalloc.a)
endif()

message("COMPILE_OPTION: COVERAGE support is ${COVERAGE}")
if(COVERAGE)
    add_compile_options(-fprofile-instr-generate)
    add_compile_options(-fcoverage-mapping)
endif()

message("COMPILE_OPTION: INLINES support is ${INLINES}")
if(INLINES)
    add_compile_options(-finline-functions)
    add_definitions(-DINLINES)
endif()

message("COMPILE_OPTION: PROF support is ${PROF}")
if(PROF)
    message(FATAL_ERROR "Profilingnot implemented")
endif()

message("COMPILE_OPTION: XLR_VALGRIND support is ${XLR_VALGRIND}")
if(XLR_VALGRIND)
    add_definitions(-DXLR_VALGRIND)
endif()

# Setup ccache
message("COMPILE_OPTION: use_ccache support is ${USE_CCACHE}")
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND AND USE_CCACHE)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif()

if (${FORCE_COLORED_OUTPUT})
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
       add_compile_options (-fdiagnostics-color=always)
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
       add_compile_options (-fcolor-diagnostics)
    endif ()
endif ()

include_directories(
    "include"
    "include/libapis"
    "include/primitives"
    "${CMAKE_BINARY_DIR}/src/include"
    "${CMAKE_BINARY_DIR}/src/include/primitives"
    "${CMAKE_BINARY_DIR}/src/include/libapis"
    "${CMAKE_BINARY_DIR}/src/lib/libprotobuf"
    "${CMAKE_BINARY_DIR}/src/lib/libservice"
    "${CMAKE_SOURCE_DIR}/src/3rd/getOpt"
    "${CMAKE_SOURCE_DIR}/src/3rd/bsd"
    "${CMAKE_SOURCE_DIR}/src/3rd/decNumber"
    "${CMAKE_SOURCE_DIR}/src/3rd/lmdb")

#
# https://developers.redhat.com/blog/2019/04/12/understanding-when-not-to-stdmove-in-c/
# Adding -Wpessimizing-move & -Wredundant-move flags to catch incorrect use of std::move
#

# Figure out why these don't work
#    -Wclobbered \
#    -Wno-coverage-mismatch \
set(COMMON_FLAGS
    "-pipe \
    -fdata-sections \
    -ffunction-sections \
    -Wall \
    -Wextra \
    -Werror \
    -Wundef \
    -Wuninitialized \
    -Wno-trigraphs \
    -Wno-unused-parameter \
    -Wunused-result \
    -D_GNU_SOURCE \
    -D_FILE_OFFSET_BITS=64 \
    -D_DEFAULT_SOURCE=1 \
    -fno-omit-frame-pointer \
    -march=corei7 \
    -fno-strict-aliasing \
    -fstrict-overflow \
    -Wframe-larger-than=65536\
    -Wno-conversion \
    -Wno-sign-conversion \
    -Wpessimizing-move \
    -Wredundant-move \
    -DHAVE_INTTYPES_H \
    -DHAVE_NETINET_IN_H \
    -D__STDC_LIMIT_MACROS \
    -DPROTOBUF_INLINE_NOT_IN_HEADERS=0")

#
#  Refer Clang documentation here. https://clang.llvm.org/docs/index.html
#  Refer Wiki https://xcalar.atlassian.net/wiki/spaces/EN/pages/8749480/Address+Sanitizer/
#
# 1. ADDRESS_SANITIZER, MEMORY_SANITIZER and THREAD_SANITIZER cannot be
# used together in the same build.
# 2. Leak Sanitizer is enabled by default with Address Sanitizer.
# 3. ADDRESS_SANITIZER & UB_SANITIZER can be enabled with the same build, but it
# can be quite resource hungry.
#

message("COMPILE_OPTION: ADDRESS_SANITIZER support is ${ADDRESS_SANITIZER}")
if(ADDRESS_SANITIZER)
    add_definitions(-DADDRESS_SANITIZER)

    set(COMMON_FLAGS "${COMMON_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
endif()

message("COMPILE_OPTION: MEMORY_SANITIZER support is ${MEMORY_SANITIZER}")
if(MEMORY_SANITIZER)
    add_definitions(-DMEMORY_SANITIZER)
    set(COMMON_FLAGS "${COMMON_FLAGS} -fsanitize=memory -fno-omit-frame-pointer")
endif()

message("COMPILE_OPTION: THREAD_SANITIZER support is ${THREAD_SANITIZER}")
if(THREAD_SANITIZER)
    add_definitions(-DTHREAD_SANITIZER)
    set(COMMON_FLAGS "${COMMON_FLAGS} -fsanitize=thread -fno-omit-frame-pointer")
endif()

message("COMPILE_OPTION: UB_SANITIZER support is ${UB_SANITIZER}")
if(UB_SANITIZER)
    add_definitions(-DUB_SANITIZER)
    set(COMMON_FLAGS "${COMMON_FLAGS} -fsanitize=bool,bounds,enum,function,nullability,nonnull-attribute,null,pointer-overflow,return,returns-nonnull-attribute,shift,unreachable,vla-bound,vptr -fno-omit-frame-pointer")
endif()

set(COMMON_CXX_FLAGS "${COMMON_FLAGS} -Wno-unused-private-field")

set(RELEASE_COMMON_CXX_FLAGS
    "-Wno-unused-but-set-variable \
    -Wno-unused-variable \
    -Wno-unused-function \
    -O2")

# C flags
set(CMAKE_C_FLAGS_DEBUG
    "-std=gnu1x \
    -Wstrict-prototypes \
    ${COMMON_FLAGS} \
    -fstack-protector-all \
    -O0 \
    -ggdb")

set(CMAKE_C_FLAGS_RELEASE
    "-std=gnu1x \
    ${COMMON_FLAGS} \
    ${CMAKE_C_FLAGS_RELEASE}")

set(CMAKE_C_FLAGS_RELWITHDEBINFO
    "-std=gnu1x \
    ${COMMON_FLAGS} \
    ${CMAKE_C_FLAGS_RELWITHDEBINFO}")

# CXX Flags
set(CMAKE_CXX_FLAGS_DEBUG
    "${COMMON_CXX_FLAGS} \
    -DDEBUG \
    -Wno-unused-const-variable \
    -fstack-protector-all \
    -Wno-unneeded-internal-declaration \
    -Wno-unused-function \
    -O0 \
    -ggdb")

set(CMAKE_CXX_FLAGS_RELEASE
    "${COMMON_CXX_FLAGS} \
    ${RELEASE_COMMON_CXX_FLAGS} \
    ${CMAKE_CXX_FLAGS_RELEASE}")

set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "${COMMON_CXX_FLAGS} \
    ${RELEASE_COMMON_CXX_FLAGS} \
    ${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")

set(COVERAGE_LINK_OPTIONS
    -fprofile-instr-generate)

set(COMMON_LIBS
    libsimnode
    liboperators
    libqa
    libcursor
    libxdb
    libmsg
    libmsgstubs # XXX this was treated specially in the automake version
    libquerymanager
    libqueryparser
    liboptimizer
    libqueryeval
    libapis_send
    libsys
    libconfig
    libapis_recv
    libtable
    libdag
    libds
    libdf
    libgetopt
    libstat
    libbc
    libbcquota
    liblog
    libstrings
    libfunctests
    libexec
    libusrnode
    libhash
    libconstants
    libxdfs
    libscalars
    libnewtupbuf
    libenum
    libusr
    libsession
    libsupport
    libcallout
    libshmsg
    libmonitor_interface
    libudf
    libudfpy
    libchild
    libparent
    libcommon
    libexport
    libruntime
    libgvm
    libapp
    liblicense
    libdurable
    libxprotobuf
    libapp
    liblocalmsg
    libchildpool
    libmsgstream
    libdemystify
    libns
    libdatapage
    libtransport
    libxccompress
    libutilxc
    libkvstore
    liblmdb
    libdecnumber)
include(genProto) # set PROTO_SRC_ROOT
set(FETCHFILE "${CMAKE_SOURCE_DIR}/bin/fetchFileFromQaDataRepo.sh")
set(GENENUM "${CMAKE_SOURCE_DIR}/bin/genenum.py")
set(GENVERSION "${CMAKE_SOURCE_DIR}/bin/genVersionSig.py")
set(GENPROTOVERSION "${CMAKE_SOURCE_DIR}/bin/genProtoVersionSig.py")
set(GENIDLVERS "${CMAKE_SOURCE_DIR}/bin/genprotoshas.sh")
set(GENEMBEDDED "${CMAKE_SOURCE_DIR}/bin/burnPubKey.py")
set(PASSWORDFILE "${CMAKE_SOURCE_DIR}/PASSWORD")

set(PROTO_VERSION_ENUM
        "${CMAKE_BINARY_DIR}/XcrpcApiVersionSignature.enum")

set(VERSION_ENUM
        "${CMAKE_BINARY_DIR}/XcalarApiVersionSignature.enum")

set(ENUM_FILES
    ${VERSION_ENUM}
    ${PROTO_VERSION_ENUM}
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
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/ChildFunFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibDsFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibDagFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibQmFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibDfFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibBcFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibSessionFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibMsgFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibKvStoreFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibQueryEvalFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibQueryParserFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibOptimizerFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibXdbFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibLogFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibRuntimeFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibOperatorsFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibAppFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibNsFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibTableFuncTestConfig.enum
    ${CMAKE_SOURCE_DIR}/src/include/test/FuncTests/LibDemystifyFuncTestConfig.enum
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
    ${CMAKE_SOURCE_DIR}/src/include/querymanager/DataflowEnums.enum)

set(ENUM_PROTO_FILES "")
foreach(enumFile ${ENUM_FILES})
    get_filename_component(enumName ${enumFile} NAME_WE)
    set(protoFile "${enumName}.proto")
    if(${enumName} STREQUAL "StatusCode")
        set(protoFile "Status.proto")
        set(enumName "Status")
    endif()
    add_custom_command(
        OUTPUT "${PROTO_DIR}/${protoFile}"
        COMMAND ${GENENUM} -i ${enumFile} -p "${PROTO_DIR}/${protoFile}"
        DEPENDS ${enumFile} ${GENENUM}
    )
    list(APPEND ENUM_PROTO_FILES "${PROTO_DIR}/${protoFile}")
endforeach(enumFile)

# not generating proto file for Constants

list(FILTER ENUM_PROTO_FILES EXCLUDE REGEX ".*Constants*.")

# not generating proto file for version control

list(FILTER ENUM_PROTO_FILES EXCLUDE REGEX ".*Version*.")


set(SERVICE_PROTO_FILES
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/Workbook.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/Service.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/Echo.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/KvStore.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/log.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/PublishedTable.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/UDF.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/Cgroup.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/memory.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/ResultSet.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/Query.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/Table.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/License.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/Dataflow.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/Sql.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/Stats.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/ColumnAttribute.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/DagNode.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/Connectors.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/SchemaLoad.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/Operator.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/App.proto"
    "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/Version.proto"
    )

xc_protobuf_link_to_dst(
        LINKED_FILES_RESULT
            LinkedSrc
        FILES
            ${SERVICE_PROTO_FILES}
)

xc_protobuf_link_to_dst(
        LINKED_FILES_RESULT
        LinkedProto
        FILES
        "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/ParentChild.proto"
        "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/ProtoMsg.proto"
        "${PROTO_SRC_ROOT}/xcalar/compute/localtypes/ProtoFieldValue.proto"
)

add_custom_target(PROTO_SERVICE_DST DEPENDS ${LinkedSrc} ${ENUM_PROTO_FILES} ${LinkedProto})

set(PROTO_SERVICE_FILES ${LinkedSrc} ${ENUM_PROTO_FILES})

if (DEFINED ENV{IS_RC} AND "$ENV{IS_RC}" STREQUAL "true")
    set(PUBKEYFILE "${CMAKE_SOURCE_DIR}/src/data/EcdsaPub.key.official.0")
else()
    set(PUBKEYFILE "${CMAKE_SOURCE_DIR}/src/data/EcdsaPub.key")
endif()

# xceBins is all of the xce executables, but doesn't install our python packages
add_custom_target(xceBins)
# Building 'xce' will build everything needed for Xcalar Compute Engine
add_custom_target(xce ALL DEPENDS xceBins)

add_custom_target(format DEPENDS clangFormat pythonFormat)
add_custom_target(formatVerify DEPENDS clangFormatVerify pythonFormatVerify)
add_custom_target(check
    COMMENT "check code format and linting"
    DEPENDS
        formatVerify
        pythonFlake8Verify
        bannedSymbolsCheck)

add_custom_target(cstyle)
add_custom_target(bannedSymbolsCheck)

add_custom_target(clangFormat)
add_custom_target(clangFormatVerify)

add_custom_target(qaData
    DEPENDS xceBins)

add_custom_target(qa
    DEPENDS xce qaData docs)

add_custom_target(sanity
    DEPENDS qa)

add_dependencies(installedObjects xceBins qaData)

link_qa_file(sanity.tst)

add_subdirectory(include)
add_subdirectory(lib)
add_subdirectory(bin)
add_subdirectory(misc)
add_subdirectory(3rd)
add_subdirectory(data)
