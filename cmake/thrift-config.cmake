# - Try to find Thrift
# Once done this will define
#  Thrift_FOUND - System has Thrift
#  Thrift_INCLUDE_DIRS - The Thrift include directories
#  Thrift_LIBRARIES - The libraries needed to use Thrift
#  Thrift_DEFINITIONS - Compiler switches required for using Thrift
find_package(PkgConfig)
pkg_check_modules(PC_THRIFT QUIET thrift)
set(Thrift_DEFINITIONS ${PC_THRIFT_CFLAGS_OTHER})
find_path(Thrift_INCLUDE_DIR thrift/generate/t_generator.h
          HINTS ${PC_THRIFT_INCLUDEDIR} ${PC_THRIFT_INCLUDE_DIRS}
	${CMAKE_INSTALL_PREFIX}
          PATH_SUFFIXES thrift  )
find_library(Thrift_LIBRARY NAMES libthrift.a thrift libthrift
             HINTS ${PC_THRIFT_LIBDIR} ${PC_THRIFT_LIBRARY_DIRS} )
include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBTHRIFT_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(Thrift DEFAULT_MSG
                  Thrift_LIBRARY Thrift_INCLUDE_DIR)
mark_as_advanced(Thrift_INCLUDE_DIR Thrift_LIBRARY )
set(Thrift_LIBRARIES ${Thrift_LIBRARY} )
set(Thrift_INCLUDE_DIRS ${Thrift_INCLUDE_DIR} )
