add_xce_executable(usrnode UsrNodeMain.cpp ChildNodeMain.cpp XcalarMain.cpp)

link_config_cfg(${CMAKE_CURRENT_SOURCE_DIR}/test-config.cfg)

include_directories(
    "${CMAKE_SOURCE_DIR}/src/lib/libstat"
    "${CMAKE_SOURCE_DIR}/src/lib/libutil")

find_package(Threads REQUIRED)
target_link_libraries(usrnode
    ${COMMON_LIBS}
    stdc++
    libstringutilxc
    xml2
    protobuf
)
if(COVERAGE)
    target_link_libraries(usrnode ${COVERAGE_LINK_OPTIONS})
endif()

install(TARGETS usrnode DESTINATION bin)
install(FILES launcher.sh launcher-master.sh slave.sh DESTINATION bin)
