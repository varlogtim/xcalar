add_xce_executable(xccli Cli.cpp)

find_package(Threads REQUIRED)
target_link_libraries(xccli
    ${COMMON_LIBS}
    stdc++
    libstringutilxc
    xml2
    protobuf
    # Xcalar libs
    libexec
)

install(TARGETS xccli DESTINATION "bin")
