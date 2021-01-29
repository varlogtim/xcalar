include_directories(
    "${CMAKE_SOURCE_DIR}/src/lib/libutil")

add_xce_library(libds STATIC Dataset.cpp BackingData.cpp AppLoader.cpp LibDatasetGvm.cpp)
target_link_libraries(libds libxdb libdatapage)
