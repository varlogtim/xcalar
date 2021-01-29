find_package(Thrift REQUIRED PATHS /opt/xcalar)
find_package(CryptoPP REQUIRED)

include_directories(
    "${Thrift_INCLUDE_DIRS}"
    "${CMAKE_SOURCE_DIR}/src/lib/libstat"
    "${CMAKE_SOURCE_DIR}/src/lib/libutil"
    "${CMAKE_BINARY_DIR}/src/bin/thrift/cppSkeleton"
)

link_directories(
    "${Thrift_LIBDIR}"
)

add_definitions(-D_THRIFT_USING_CLANG_LIBCXX=1 -D_THRIFT_USING_MICROSOFT_STDLIB=0 -DT_GLOBAL_DEBUG_VIRTUAL=0)
add_definitions(-DUSE_BOOST_THREAD=0 -DUSE_STDTHREADS=0 -DUSE_STD_THREAD=1)

add_xce_executable(xcmgmtd MgmtDaemon.cpp SysStatsHelper.cpp)
target_link_libraries(xcmgmtd
    ${CryptoPP_Libraries}
    "${Thrift_LIBRARIES}"
    ${COMMON_LIBS}
    stdc++
    # Xcalar libs
    libsys
    libconfig
    libnewtupbuf
    libapis_send
    libapis_recv
    libstat
    libutilxc
    libstringutilxc
    libsupport
    libsimnode
    libthriftskel
    editline
)

install(TARGETS xcmgmtd DESTINATION "bin")
add_dependencies(xcmgmtd thriftPyFiles)
