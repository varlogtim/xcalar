# XXX there's some wonky stuff going on here with the INSTALL_DEPENDS.
# See src/bin/sdk/xdp/CMakeLists.txt for more info
add_xce_py_package(controllerPackage
  CODE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
  INSTALL_TARGET controllerInstall
  INSTALL_DEPENDS xc2Install)
add_dependencies(xce controllerInstall)
