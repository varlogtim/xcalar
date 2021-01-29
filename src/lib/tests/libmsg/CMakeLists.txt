link_qa_file(sanity.tst)
link_config_cfg(${CMAKE_CURRENT_BINARY_DIR}/test-config.cfg)

add_xce_test_executable(libmsgsanity LibMsgTest.cpp)
target_link_libraries(libmsgsanity ${TEST_LIBS})
