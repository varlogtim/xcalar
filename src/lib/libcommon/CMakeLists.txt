# Only include version on the commandline here
add_definitions(-DXLRVERSIONPRETTY=\"${XLRVERSIONPRETTY}\" -DXLRVERSION=\"${XLRVERSION}\")
add_xce_library(libcommon STATIC InitTeardown.cpp Version.cpp ClusterGen.cpp)
target_link_libraries(libcommon
    libcallout
    libqueryparser
    liblog
    libusrnode
    libapis_send
    libapis_recv
    libkvstore
    libusr
    libsession
    libquerymanager
    liboptimizer
    libqueryeval
    libsupport
    libudf
    libexport
    libxprotobuf
    liblocalmsg
    libns)
