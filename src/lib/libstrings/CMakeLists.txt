set(BOYER_MOORE_SRC "${CMAKE_SOURCE_DIR}/src/3rd/boyerMoore/BoyerMoore.c")
set(LOCAL_BOYER_MOORE_SRC "${CMAKE_CURRENT_BINARY_DIR}/BoyerMoore.c")
add_custom_command(
    OUTPUT ${LOCAL_BOYER_MOORE_SRC}
    COMMAND ${CMAKE_COMMAND} -E create_symlink
    ${BOYER_MOORE_SRC} BoyerMoore.c
    DEPENDS ${BOYER_MOORE_SRC})
add_xce_library(libstrings STATIC Strings.cpp ${LOCAL_BOYER_MOORE_SRC} ${GLOBAL_DEPS})
