add_custom_target(sdkPackage)
add_dependencies(installedObjects sdkPackage)

add_subdirectory(core)
add_subdirectory(xdp)
add_subdirectory(xpu)
add_subdirectory(docs)
add_subdirectory(controller)
