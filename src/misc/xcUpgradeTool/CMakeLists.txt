add_xce_executable(xcUpgradeTool XcUpgradeTool.cpp)

include_directories(
    "${CMAKE_SOURCE_DIR}/src/lib/libstat"
    "${CMAKE_SOURCE_DIR}/src/lib/libutil"
    "${CMAKE_SOURCE_DIR}/src/lib/libdag"
    )

find_package(Threads REQUIRED)
target_link_libraries(xcUpgradeTool
    ${COMMON_LIBS}
    stdc++
)

install(TARGETS xcUpgradeTool DESTINATION "bin")
