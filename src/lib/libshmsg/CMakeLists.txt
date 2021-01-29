include_directories("${CMAKE_SOURCE_DIR}/src/include/child")
add_xce_library(libshmsg STATIC RingBuffer.cpp SharedMemory.cpp)
target_link_libraries(libshmsg
    rt)
