set(LINKED_FILES
    "${CMAKE_SOURCE_DIR}/src/3rd/fnv/Fnv.cpp"
    "${CMAKE_SOURCE_DIR}/src/3rd/crc/crc32c.cpp")

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
add_xce_library(libhash STATIC Hash.cpp ${LINKED_LOCALS})
