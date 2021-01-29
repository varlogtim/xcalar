set(SMALL_FILES "")

foreach(ii RANGE 299)
    set(SMALL_FILES
        ${SMALL_FILES}
        ${CMAKE_CURRENT_BINARY_DIR}/file${ii}
    )
endforeach(ii)

set(ii 0)
foreach(SMALL_FILE ${SMALL_FILES})
    add_custom_command(
        OUTPUT ${SMALL_FILE}
        COMMAND ${CMAKE_COMMAND} -E echo "fileNum" > ${SMALL_FILE}
        COMMAND ${CMAKE_COMMAND} -E echo "${ii}" >> ${SMALL_FILE}
    )
    math(EXPR ii "${ii} + 1")
endforeach(SMALL_FILE)

add_custom_target(smallFiles DEPENDS ${SMALL_FILES})
add_dependencies(qaData smallFiles)
