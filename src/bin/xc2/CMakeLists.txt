
add_xce_py_package(xc2Package
    CODE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
    INSTALL_TARGET xc2Install
    INSTALL_DEPENDS utilInstall)
add_dependencies(xce xc2Install)
