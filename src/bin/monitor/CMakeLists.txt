add_xce_executable(xcmonitor monitor.cpp)

link_qa_file(monitorBig.sh)

target_link_libraries(xcmonitor
    ${COMMON_LIBS}
    libmonitor_interface
    libsys
    libsimnode
    libutilxc
)

install(TARGETS xcmonitor DESTINATION "bin")
