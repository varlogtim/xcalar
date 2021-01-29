include_directories(
    "${CMAKE_SOURCE_DIR}/src/lib/libutil")

# Disable optimization for SerDes, see bug 10563
set_source_files_properties(KvStoreDurable.cpp PROPERTIES COMPILE_FLAGS -O0)

add_xce_library(libkvstore STATIC KvStore.cpp KvStoreLocal.cpp)

target_link_libraries(libkvstore liblmdb)
