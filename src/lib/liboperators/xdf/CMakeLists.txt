include_directories("${CMAKE_SOURCE_DIR}/src/lib/liboperators")
add_xce_library(libxdfs STATIC TransformFns.cpp AggregateFns.cpp ConditionFns.cpp CommonFns.cpp)
target_link_libraries(libxdfs
    libsys
    libstrings)
