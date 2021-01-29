add_xce_executable(ioBenchmark ioBenchmark.cpp)

target_link_libraries(ioBenchmark
    ${COMMON_LIBS}
    ${COMMON_LIBS} # To get around circular dependncies
)

install(TARGETS ioBenchmark DESTINATION "bin")
