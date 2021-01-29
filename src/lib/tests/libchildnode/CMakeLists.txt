link_qa_file(sanity.tst)

add_xce_test_executable(libchildnodetest ParentChildTest.cpp)
target_link_libraries(libchildnodetest ${TEST_LIBS})
target_include_directories(libchildnodetest
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/lib/libparent
    ${CMAKE_SOURCE_DIR}/src/lib/libchild
    ${CMAKE_SOURCE_DIR}/src/lib/libudf
)
