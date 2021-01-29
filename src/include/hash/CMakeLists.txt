set(LINKED_HEADERS
    ${CMAKE_SOURCE_DIR}/src/3rd/fnv/Fnv.h)

set(LOCAL_HEADERS "")

foreach(headerSource ${LINKED_HEADERS})
    get_filename_component(localFile ${headerSource} NAME)
    add_custom_command(
        OUTPUT ${localFile}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${headerSource} ${localFile}
        DEPENDS ${headerSource})
    list(APPEND LOCAL_HEADERS ${localFile})
endforeach(headerSource)

add_custom_target(GENERATED_HEADERS_HASH
    DEPENDS ${LOCAL_HEADERS})
