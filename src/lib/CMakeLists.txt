link_qa_file(sanity.tst)

# Clang-format all private headers
add_custom_target(clangFormatPrivateHeaders
    COMMAND
        # Exclude durable files (not elegant)
        find ${CMAKE_CURRENT_SOURCE_DIR} -name "*.h" | grep -v "Durable" | xargs -n1 -P0 clang-format -i
    VERBATIM
    COMMENT
        "formatting header files")
add_dependencies(clangFormat clangFormatPrivateHeaders)

# Clang-format all headers
add_custom_target(clangFormatVerifyPrivateHeaders
    COMMAND
        # Exclude durable files (not elegant)
        find ${CMAKE_CURRENT_SOURCE_DIR} -name "*.h" | grep -v "Durable" | xargs -I {} -n1 -P0 bash -c "clang-format {} | diff -q {} /dev/fd/0"
    VERBATIM
    COMMENT
        "formatting header files")
add_dependencies(clangFormatVerify clangFormatVerifyPrivateHeaders)


add_subdirectory(libprotobuf)
add_subdirectory(libstrings)
add_subdirectory(tests)
add_subdirectory(libmoninterface)
add_subdirectory(libparent)
add_subdirectory(libdf)
add_subdirectory(libhash)
add_subdirectory(libenum)
add_subdirectory(libmsgstream)
add_subdirectory(libxdb)
add_subdirectory(libutil)
add_subdirectory(liblicense)
add_subdirectory(libusr)
add_subdirectory(libsession)
add_subdirectory(libqueryeval)
add_subdirectory(libexport)
add_subdirectory(liblocalmsg)
add_subdirectory(libmsg)
add_subdirectory(libcallout)
add_subdirectory(libstat)
add_subdirectory(libgvm)
add_subdirectory(libkvstore)
add_subdirectory(liblog)
add_subdirectory(libchild)
add_subdirectory(libapis)
add_subdirectory(libusrnode)
add_subdirectory(libds)
add_subdirectory(libruntime)
add_subdirectory(libexec)
add_subdirectory(libconstants)
add_subdirectory(libshmsg)
add_subdirectory(libdag)
add_subdirectory(liboperators)
add_subdirectory(libcursor)
add_subdirectory(libcommon)
add_subdirectory(libsupport)
add_subdirectory(libfaulthandler)
add_subdirectory(libsys)
add_subdirectory(libbcquota)
add_subdirectory(libscalars)
add_subdirectory(libconfig)
add_subdirectory(libapp)
add_subdirectory(libnewtupbuf)
add_subdirectory(libqueryparser)
add_subdirectory(libquerymanager)
add_subdirectory(liboptimizer)
add_subdirectory(libudf)
add_subdirectory(libbc)
add_subdirectory(libchildpool)
add_subdirectory(libdemystify)
add_subdirectory(libns)
add_subdirectory(libdurable)
add_subdirectory(libdatapage)
add_subdirectory(libtransport)
add_subdirectory(libxccompress)
add_subdirectory(libtable)
add_subdirectory(libservice)
