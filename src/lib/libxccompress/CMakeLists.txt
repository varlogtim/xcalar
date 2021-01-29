find_package(Snappy)

add_xce_library(libxccompress STATIC XcSnappy.cpp)

target_link_libraries(libxccompress
  ${SNAPPY_LIBRARIES})