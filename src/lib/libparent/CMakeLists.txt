include_directories("${CMAKE_SOURCE_DIR}/src/include/parent" "${CMAKE_SOURCE_DIR}/src/lib/libchild")
add_xce_library(libparent STATIC Parent.cpp ParentChild.cpp ParentStats.cpp)
target_link_libraries(libparent
    libshmsg)
