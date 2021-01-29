# - Try to find CryptoPP
# Once done this will define
#  CryptoPP_FOUND - System has CryptoPP
#  CryptoPP_INCLUDE_DIRS - The CryptoPP include directories
#  CryptoPP_LIBRARIES - The libraries needed to use CryptoPP
#  CryptoPP_DEFINITIONS - Compiler switches required for using CryptoPP
find_package(PkgConfig)
pkg_check_modules(PC_CRYPTOPP QUIET CryptoPP)
set(CryptoPP_DEFINITIONS ${PC_CRYPTOPP_CFLAGS_OTHER})
find_path(CryptoPP_INCLUDE_DIR cryptopp/cryptlib.h
          HINTS ${PC_CRYPTOPP_INCLUDEDIR} ${PC_CRYPTOPP_INCLUDE_DIRS}
          PATH_SUFFIXES cryptopp )
find_library(CryptoPP_LIBRARY NAMES libcryptopp.a cryptopp libcryptopp
             HINTS ${PC_CRYPTOPP_LIBDIR} ${PC_CRYPTOPP_LIBRARY_DIRS} )
include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBCRYPTO_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(CryptoPP DEFAULT_MSG
                  CryptoPP_LIBRARY CryptoPP_INCLUDE_DIR)
mark_as_advanced(CryptoPP_INCLUDE_DIR CryptoPP_LIBRARY )
set(CryptoPP_LIBRARIES ${CryptoPP_LIBRARY} )
set(CryptoPP_INCLUDE_DIRS ${CryptoPP_INCLUDE_DIR} )
