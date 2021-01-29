link_qa_file(sanity.tst)
link_qa_file(libapissanity.sh)

link_config_cfg(${CMAKE_CURRENT_BINARY_DIR}/test-config.cfg)

add_xce_test_executable(libapisTestDriver LibApisTests.cpp)
target_link_libraries(libapisTestDriver ${TEST_LIBS})
