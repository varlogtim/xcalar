# XXX there's some wonky stuff going on here with the INSTALL_DEPENDS.
# See src/bin/sdk/xdp/CMakeLists.txt for more info
add_xce_py_package(utilPackage
    CODE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
    INSTALL_TARGET utilInstall
    INSTALL_DEPENDS xpuInstall)
add_dependencies(xce utilInstall)
