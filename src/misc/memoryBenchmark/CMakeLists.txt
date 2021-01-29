add_xce_executable(memoryBenchmark memoryBenchmark.cpp)

target_link_libraries(memoryBenchmark
    ${COMMON_LIBS}
    ${COMMON_LIBS} # To get around circular dependncies
)

install(TARGETS memoryBenchmark DESTINATION "bin")
