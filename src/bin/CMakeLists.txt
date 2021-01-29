link_qa_file(sanity.tst)

add_subdirectory(usrnode)
add_subdirectory(mgmtd)
add_subdirectory(thrift)
add_subdirectory(cli)
add_subdirectory(legacyJsClient)
add_subdirectory(jsClient)
add_subdirectory(monitor)
add_subdirectory(childnode)
add_subdirectory(tests)
add_subdirectory(init.d)
add_subdirectory(ldap-installer)
add_subdirectory(licenseTools)
add_subdirectory(sdk)
add_subdirectory(util)
add_subdirectory(xc2)


add_custom_target(pythonFormat
    COMMAND
        # -i: in-place
        # -r: recursively find all .py files
        # -p: run in parallel
        yapf -i -r -p ${CMAKE_CURRENT_SOURCE_DIR}
    VERBATIM
    COMMENT
        "formatting python files"
)

add_custom_target(pythonFormatVerify
    COMMAND
        # -r: recursively find all .py files
        # -p: run in parallel
        # --diff: yapf to return non-zero on any diff
        yapf -r -p --diff ${CMAKE_CURRENT_SOURCE_DIR}
    VERBATIM
    COMMENT
        "validating python file formatting"
)

add_custom_target(pythonFlake8Verify
    COMMAND
        flake8 ${CMAKE_CURRENT_SOURCE_DIR}
    VERBATIM
    COMMENT
        "validating python code quality"
)
