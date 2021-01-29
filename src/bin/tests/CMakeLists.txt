set(MGMTD_FILES0
    ${CMAKE_CURRENT_SOURCE_DIR}/mgmttestactual.js.header
    ${CMAKE_SOURCE_DIR}/src/3rd/jquery/jquery.js
    ${CMAKE_SOURCE_DIR}/src/3rd/thrift/thrift.js
)

set(MGMTD_FILES1
    ${CMAKE_BINARY_DIR}/src/bin/legacyJsClient/jsPackage/XcalarApiServiceAsync.js
    ${CMAKE_BINARY_DIR}/src/bin/legacyJsClient/jsPackage/XcalarApi.js
    ${CMAKE_CURRENT_SOURCE_DIR}/MgmtTest.js
)

set(MGMTD_STATIC
    ${MGMTD_FILES0}
    ${MGMTD_FILES1}
)

set(MGMTD_FILES
    ${MGMTD_FILES0}
    ${CMAKE_BINARY_DIR}/src/bin/thrift/js/*.js
    ${MGMTD_FILES1}
)

build_qa_retina(testRetina)
link_qa_file(sanity.tst)
link_qa_file(mgmtdtest.sh)
link_qa_file(PyExecOnLoadTest.py)
link_qa_file(cliTest.sh)
link_qa_file(cliCmds.sh)
link_qa_file(cliTestSuites)
link_qa_file(localSystemTest.sh)
link_qa_file(runXceTests.sh)
link_qa_file(monitorTest.sh)
link_qa_file(monitorBigTest.sh)
link_qa_file(licCheckTest.sh)
link_qa_file(sessionReplayTest.sh)
link_qa_file(genProtoTest.sh)
link_qa_file(portScanTest.sh)
link_qa_file(mgmttestdriver.js)
link_config_cfg(${CMAKE_CURRENT_BINARY_DIR}/test-config.cfg)

add_custom_command(
    OUTPUT
        ${CMAKE_CURRENT_BINARY_DIR}/test-config-mgmtd.cfg
    COMMAND
        ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_BINARY_DIR}/test-config.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/test-config-mgmtd.cfg
    DEPENDS
        ${CMAKE_CURRENT_BINARY_DIR}/test-config.cfg
)

add_subdirectory(pyTest)
add_subdirectory(pyTestNew)

add_dependencies(qa MGMTD_TEST)
add_custom_target(MGMTD_TEST
    DEPENDS mgmttestdriver.js mgmttestactual.js ${CMAKE_CURRENT_BINARY_DIR}/test-config-mgmtd.cfg ${REPO_DATA})

add_custom_command(
    OUTPUT
        mgmttestactual.js
    COMMAND
        cat ${MGMTD_FILES} > mgmttestactual.js
    DEPENDS
        ${MGMTD_STATIC} ${CMAKE_BINARY_DIR}/src/bin/legacyJsClient/package.stamp
)
# add_subdirectory(systemTests)
