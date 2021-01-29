# XXX there's some wonky stuff going on here.
# 'pip' isn't threadsafe, so we need to make sure our INSTALL_DEPENDS tree works
# out such that the order is linear. Otherwise CMake will try to install 2
# packages at the same time and will corruptly only install 1 of them.
#
# This issue may be exclusive to the 'develop' install. (pip install -e)
#
# It might make more sense to just build the wheels and then install
# all of them at once. This would be very slow though. Especially because we
# don't know what files are in a package ahead of time, so we would have to
# always do this, instead of using source file mtime.
#
# Another options would be to wrap pip with something mutex-like to ensure that
# only 1 thing is built at a time. This seems even more evil.
#
# The current build order is:
# thrift
# sdk/core
# sdk/xdp
# sdk/xpu
# util
# xc2
# controller
add_xce_py_package(xdpPackage
    CODE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
    INSTALL_TARGET xdpInstall
    INSTALL_DEPENDS xceCoreInstall)
add_dependencies(xce xdpInstall)

set(logLevelEnum "${CMAKE_SOURCE_DIR}/src/include/LogLevel.enum")
set(logLevelEnumOutputFile "${CMAKE_CURRENT_SOURCE_DIR}/xcalar/external/LogLevel.py")

add_custom_command(
    OUTPUT ${logLevelEnumOutputFile}
    COMMAND ${GENENUM} -i ${logLevelEnum} -y ${logLevelEnumOutputFile}
    DEPENDS ${logLevelEnum})

add_custom_target(GENERATED_PYTHON_ENUMS
    DEPENDS ${logLevelEnumOutputFile})
