add_xce_test_executable(logdump LogDump.cpp LogGenTestData.cpp)

include_directories(
    "${CMAKE_SOURCE_DIR}/src/lib/libstat"
    "${CMAKE_SOURCE_DIR}/src/lib/libutil"
    "${CMAKE_SOURCE_DIR}/src/lib/libxdb"
    "${CMAKE_SOURCE_DIR}/src/lib/libsession"
    "${CMAKE_SOURCE_DIR}/src/lib/libkvstore"
    "${CMAKE_SOURCE_DIR}/src/lib/libexport"
    )

find_package(Threads REQUIRED)
target_link_libraries(logdump
    ${COMMON_LIBS}
    stdc++
    libstringutilxc
    xml2
    protobuf
)
