cmake_minimum_required(VERSION 3.0.0)
project(xcalar VERSION 0.0.0)

enable_testing()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'Release' as none was specified.")
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo")
endif()

if(CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++11")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-conversion-null")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++11")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-undef -Wno-deprecated-register -Wno-unknown-warning-option -Wno-unused-command-line-argument")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libstdc++")
  if(COVERAGE)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-instr-generate" )
  endif()
  add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)  # For all the gory details, see: https://gcc.gnu.org/onlinedocs/libstdc++/manual/using_dual_abi.html
  add_definitions(-DGPR_FORBID_UNREACHABLE_CODE=0)  # To work around an #if in grpc/impl/codegen/port_platform.h
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

  find_program(XIAR xiar)
  if(XIAR)
    set(CMAKE_AR "${XIAR}")
  endif(XIAR)
  MARK_AS_ADVANCED(XIAR)

  find_program(XILD xild)
  if(XILD)
    set(CMAKE_LINKER "${XILD}")
  endif(XILD)
  mark_as_advanced(XILD)

  # ICC will default to -fp-model fast=1, which performs value-unsafe optimizations which will
  # cause pbrt_test to fail. For safety, -fp-model precise is explicitly set here by default.
  set(FP_MODEL "precise" CACHE STRING "The floating point model to compile with.")
  set_property(CACHE FP_MODEL PROPERTY STRINGS "precise" "fast=1" "fast=2")

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fp-model ${FP_MODEL}")
endif()

set(PKG_CONFIG_USE_CMAKE_PREFIX_PATH ON)

include(CTest)
enable_testing()

# Build compilation directory
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
set(PKGLANG "en")
set(OS "linux") # OS; linux/win/esc
set(ASPACE "user") # address space; kernel/user
include(CPack)

# Set up 'config.h'
include(CheckIncludeFile)
check_include_file("archive.h" HAVE_ARCHIVE_H)
check_include_file("archive3.h" HAVE_ARCHIVE3_H)
find_path(NETINET_FILE netinet/in.h)
check_include_file(netinet/in.h HAVE_NETINET_IN_H)
include_directories(${NETINET_FILE})

include(CheckTypeSize)
check_type_size("double" SIZEOF_DOUBLE)
check_type_size("fd_set" SIZEOF_FD_SET)
check_type_size("float" SIZEOF_FLOAT)
check_type_size("int" SIZEOF_INT)
check_type_size("long double" SIZEOF_LONG_DOUBLE)
check_type_size("long int" SIZEOF_LONG_INT)
# XXX  none of these work. Adding a CMAKE_EXTRA_INCLUDE_FILES hasn't helped
check_type_size("iovec" SIZEOF_STRUCT_IOVEC)
check_type_size("struct sockaddr_in" SIZEOF_STRUCT_SOCKADDR_IN)
check_type_size("va_list" SIZEOF_VA_LIST)

set(PREFIX "/opt/xcalar")
set(CMAKE_INSTALL_RPATH $ORIGIN/../lib)

configure_file(config.h.cmake src/include/config.h)
include_directories(${CMAKE_BINARY_DIR}/src/include)
add_definitions(-DHAVE_CONFIG_H)
set(ENV{PKG_CONFIG_PATH} "${PREFIX}/lib/pkgconfig:${PREFIX}/lib64/pkgconfig:$ENV{PKG_CONFIG_PATH}")

include_directories("${PREFIX}/include")
link_directories(
    "${PREFIX}/lib64"
    "${PREFIX}/lib"
)

# Set environmental variables
execute_process(
    COMMAND hostname -s
    OUTPUT_VARIABLE HOSTNAME
    OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
    COMMAND whoami
    OUTPUT_VARIABLE WHOAMI
    OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
    COMMAND git rev-parse --verify HEAD
    COMMAND cut -c1-8
    OUTPUT_VARIABLE GIT_SHA
    OUTPUT_STRIP_TRAILING_WHITESPACE)

set(GIT_DIR_TMP "$ENV{GIT_DIR}")
set(GIT_WORK_TREE_TMP "$ENV{GIT_DIR}")

if (DEFINED ENV{GIT_WORK_TREE})
    unset(ENV{GIT_WORK_TREE})
endif()

if (DEFINED ENV{GIT_DIR})
    set(ENV{GIT_DIR} "$ENV{GIT_DIR}/modules/xcalar-idl")
endif()

execute_process(
    COMMAND git rev-parse --verify HEAD
    COMMAND cut -c1-8
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/xcalar-idl"
    OUTPUT_VARIABLE GIT_SHA_IDL
    OUTPUT_STRIP_TRAILING_WHITESPACE)

set(ENV{GIT_DIR} "${GIT_DIR_TMP}")
set(ENV{GIT_WORK_TREE} "${GIT_WORK_TREE_TMP}")

execute_process(
    COMMAND cat ${CMAKE_CURRENT_SOURCE_DIR}/VERSION
    OUTPUT_VARIABLE XLRVERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE)
set(BUILD_NUMBER 1)
if(DEFINED ENV{BUILD_NUMBER})
    set(BUILD_NUMBER $ENV{BUILD_NUMBER})
endif()

if (${GIT_SHA} STREQUAL ${GIT_SHA_IDL})
    message(FATAL_ERROR "GIT_SHA & GIT_SHA_IDL are same!")
endif()

set(XLRVERSIONPRETTY "${XLRVERSION}-${BUILD_NUMBER}-${WHOAMI}-${GIT_SHA}-${GIT_SHA_IDL}")

# We want to manually specify the built objects; without the following line,
# CMake will automatically build all targets before installation.
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)
add_custom_target(installedObjects)

# Setup test macro
macro(add_xc_test testName)
    add_test(NAME testName
             COMMAND testName)
endmacro()

set(CMAKE_MODULE_PATH
    ${CMAKE_MODULE_PATH}
    "${CMAKE_SOURCE_DIR}/cmake"
    "${CMAKE_SOURCE_DIR}/cmake/modules"
    )

include(AddXcalar)
include(Qa)

add_subdirectory(src)
add_subdirectory(bin)
add_subdirectory(conf)
add_subdirectory(scripts)

link_qa_file(sanity.tst)

install(FILES NOTICE
        RENAME NOTICE.XCE
        PERMISSIONS OWNER_READ OWNER_WRITE
                    GROUP_READ
                    WORLD_READ
        DESTINATION share)

add_custom_command(
    OUTPUT "${CMAKE_SOURCE_DIR}/cmake_binary_dir"
    COMMAND ${CMAKE_COMMAND} -E create_symlink
        "${CMAKE_BINARY_DIR}" "${CMAKE_SOURCE_DIR}/cmake_binary_dir")

add_custom_target(BINARY_LINK ALL
    DEPENDS "${CMAKE_SOURCE_DIR}/cmake_binary_dir")

add_dependencies(xceBins BINARY_LINK)
