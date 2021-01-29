set(LINKED_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/${OS}/${ASPACE}/Socket.cpp")

set(LINKED_LOCALS "")
foreach(linkedFile ${LINKED_FILES})
    get_filename_component(localFile ${linkedFile} NAME)
    add_custom_command(
        OUTPUT ${localFile}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${linkedFile} ${localFile}
        DEPENDS ${linkedFile})
    list(APPEND LINKED_LOCALS "${CMAKE_CURRENT_BINARY_DIR}/${localFile}")
endforeach(linkedFile)

add_xce_library(libsys STATIC
	Check.cpp
	XSyslog.cpp
	${LINKED_LOCALS})
target_link_libraries(libsys libmsg libxccompress)
