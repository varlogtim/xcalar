find_package(PythonLib REQUIRED)

include_directories("${PYTHON_INCLUDE_DIRS}" "${CMAKE_SOURCE_DIR}/src/lib/libudf/python")
add_xce_library(libapp STATIC
    AppMgr.cpp
    App.cpp
    AppInstance.cpp
    AppGroup.cpp
    AppGroupMgr.cpp
    AppHost.cpp
    PythonApp.cpp
    AppStats.cpp
    AppGroupMgrGvm.cpp
    AppMgrGvm.cpp
    SystemApp.cpp
    XpuCommStream.cpp)

target_link_libraries(
    libapp
    ${PYTHON_LIBRARIES}
    dl
    util)

