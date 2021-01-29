# - Try to find MapRClient
# Once done this will define
#  MapRClient_FOUND - System has MapRClient
#  MapRClient_INCLUDE_DIRS - The MapRClient include directories
#  MapRClient_LIBRARIES - The libraries needed to use MapRClient
#  MapRClient_DEFINITIONS - Compiler switches required for using MapRClient
find_package(PkgConfig)
pkg_check_modules(PC_CRYPTOPP QUIET MapRClient)
set(MapRClient_DEFINITIONS "")
find_path(MapRClient_INCLUDE_DIR hdfs.h
          PATHS /opt/mapr/hadoop
          PATH_SUFFIXES "hadoop-2.7.0/include")
find_library(MapRClient_LIBRARY
             NAMES MapRClient libMapRClient
             PATHS /opt/mapr
             PATH_SUFFIXES "lib")
include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(MapRClient DEFAULT_MSG
                  MapRClient_LIBRARY MapRClient_INCLUDE_DIR)
mark_as_advanced(MapRClient_INCLUDE_DIR MapRClient_LIBRARY )
set(MapRClient_LIBRARIES ${MapRClient_LIBRARY} )
set(MapRClient_INCLUDE_DIRS ${MapRClient_INCLUDE_DIR} )
