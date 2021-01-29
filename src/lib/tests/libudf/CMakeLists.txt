find_package(PythonLib REQUIRED)
link_qa_file(sanity.tst)
link_config_cfg(${CMAKE_CURRENT_BINARY_DIR}/test-config.cfg)
include_directories("${PYTHON_INCLUDE_DIRS}")

add_xce_test_executable(libudftest UdfTests.cpp)
target_link_libraries(libudftest ${TEST_LIBS})
target_include_directories(libudftest
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/lib/libudf)


add_xce_test_executable(libudfpytest UdfPyTests.cpp)
target_link_libraries(libudfpytest ${TEST_LIBS})
target_include_directories(libudfpytest
    PRIVATE
    "${PYTHON_INCLUDE_DIRS}"
    ${CMAKE_SOURCE_DIR}/src/lib/libudf/python
    ${CMAKE_SOURCE_DIR}/src/lib/libudf)

