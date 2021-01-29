find_package(PkgConfig)
pkg_check_modules(ARROW REQUIRED arrow)
pkg_check_modules(PARQUET REQUIRED parquet)

add_definitions(-Dnssv_CPP11=1)

add_xce_executable(xcParquetReader parquetReader.cpp)
target_link_libraries(xcParquetReader
    ${PARQUET_LIBRARIES}
    ${ARROW_LIBRARIES}
    ${COMMON_LIBS}
    ${COMMON_LIBS} # To get around circular dependncies
)

install(TARGETS xcParquetReader DESTINATION bin)
