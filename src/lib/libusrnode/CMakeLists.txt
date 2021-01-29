include_directories(
    "${CMAKE_SOURCE_DIR}/src/lib/libstat")
add_xce_library(libusrnode STATIC UsrNode.cpp UsrNodeMonInterface.cpp)
target_link_libraries(libusrnode
    libutilxc
    liboperators
    libmonitor_interface)
