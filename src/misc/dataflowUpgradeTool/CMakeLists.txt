find_package(Thrift REQUIRED PATHS /opt/xcalar)

add_xce_test_executable(upgradeTool XcDataFlowUpgradeEntry.cpp)

include_directories(
    "${CMAKE_SOURCE_DIR}/src/lib/libstat"
    "${CMAKE_SOURCE_DIR}/src/lib/libutil"
    "${CMAKE_SOURCE_DIR}/src/lib/libdag"
    )

find_package(Threads REQUIRED)
target_link_libraries(upgradeTool
    ${COMMON_LIBS}
    stdc++
    ${Thrift_LIBRARIES}
    #parquet
    #arrow
)
