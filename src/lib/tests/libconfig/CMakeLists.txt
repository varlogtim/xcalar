link_qa_file(sanity.tst)
link_config_cfg(${CMAKE_CURRENT_BINARY_DIR}/test-config.cfg)

add_xce_test_executable(libconfigtest LibConfigTest.cpp)
target_link_libraries(libconfigtest ${TEST_LIBS})
target_include_directories(libconfigtest
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/lib/libconfig)
