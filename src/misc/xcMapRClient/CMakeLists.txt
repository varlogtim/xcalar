find_package(MapRClient REQUIRED)
include_directories(${MapRClient_INCLUDE_DIR})

add_xce_executable(xcMapRClient XcMapRClient.cpp)
target_link_libraries(xcMapRClient
    stdc++
    jansson
    dl
    -Wl,-rpath,/opt/mapr/lib
# XXX We don't directly link to the MapRClient. Instead, we manually load
# symbols from the library at runtime.
#   ${MapRClient_LIBRARIES}
#   -Wl,--allow-shlib-undefined # Some symbols are undefined in MapRClient
)

install(TARGETS xcMapRClient DESTINATION "bin")
