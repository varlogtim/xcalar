include_directories(
    "${CMAKE_SOURCE_DIR}/src/lib/libutil")

add_xce_library(libquerymanager STATIC QueryManager.cpp)
