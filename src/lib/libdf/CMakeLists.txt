find_package(PkgConfig)
pkg_check_modules(ARROW REQUIRED arrow)
pkg_check_modules(PARQUET REQUIRED parquet)

find_package(Thrift REQUIRED PATHS /opt/xcalar)


set(LINKED_FILES
    "${CMAKE_SOURCE_DIR}/src/3rd/utf8verify/Utf8Verify.cpp"
    "${CMAKE_SOURCE_DIR}/src/3rd/utf8verify/Utf8Verify.h")

set(LINKED_LOCALS "")
foreach(linkedFile ${LINKED_FILES})
    get_filename_component(localFile ${linkedFile} NAME)
    add_custom_command(
        OUTPUT ${localFile}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${linkedFile} ${localFile}
        DEPENDS ${linkedFile})
    list(APPEND LINKED_LOCALS ${CMAKE_CURRENT_BINARY_DIR}/${localFile})
endforeach(linkedFile)


add_definitions(-Dnssv_CPP11=1)
include_directories(
  "${Thrift_INCLUDE_DIRS}"
  "${CMAKE_CURRENT_BINARY_DIR}"
  ${ARROW_INCLUDEDIR}
  ${PARQUET_INCLUDEDIR}
)

add_xce_library(libdf STATIC DataFormat.cpp DataFormatJson.cpp DataFormatCsv.cpp DataFormatSql.cpp DataFormatParquet.cpp TypeConversion.cpp FatPointer.cpp ${LINKED_LOCALS})

target_link_libraries(libdf
    libutilxc
    libdecnumber
    ${ARROW_LIBRARIES}
    ${Thrift_LIBRARIES}
    ${PARQUET_LIBRARIES}
)

