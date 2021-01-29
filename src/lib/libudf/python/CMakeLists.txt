find_package(PythonLib REQUIRED)
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

include_directories(
    "${PYTHON_INCLUDE_DIRS}"
    ${CMAKE_CURRENT_BINARY_DIR}
    "${CMAKE_SOURCE_DIR}/src/lib/libudf"
    "${CMAKE_SOURCE_DIR}/src/include/child")

add_xce_library(libudfpy STATIC
    UdfPython.cpp
    UdfPythonImpl.cpp
    UdfPyFunction.cpp
    UdfPyError.cpp
    UdfPyChild.cpp
    UdfPyXcalar.cpp
    SchemaProcessor.cpp
    PageWriter.cpp
    ${LINKED_LOCALS})

target_link_libraries(
    libudfpy
    ${PYTHON_LIBRARIES}
    dl
    util
    )
