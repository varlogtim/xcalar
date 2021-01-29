include_directories("${CMAKE_SOURCE_DIR}/src/include/child" "${CMAKE_SOURCE_DIR}/src/include/parent")
add_xce_library(libchild STATIC Child.cpp)
target_link_libraries(libchild
    libapp)
