set(STATIC_SCRIPTS
    xcasup.sh
    Support.py
    default.py
    support-generate.sh
    load.py
    list_files_app.py
    batch_csv_load.py
    schema_discover_load.py
    exportHost.py
    verify.py
    writeFileAllNodes.py
    Depends.py
    statsCollectd.py
    statsCollector.py
    vpFormatter.py
    vpJsonSudfGen.py
    vpXmlSudfGen.py
    statsPlugin.cfg
    system-info.sh
    cepudf.py
    fieldFuncTestsClean.sh
    fieldFuncTestsRun.sh
    fieldFuncTestsConfig.sh
    XdbSerDes.py
    targetMgr.py
    driverMgr.py
    parquetApp.py
    cgroupsMgr.py
    queryToDF2Upgrade.py
    stringSeed.py
    xcalar_top.py
    unstringify.py
	dataflow_trace.py
    system_stats_app.py
    gather_stats.py
    xcalarjdbc.js
    xcalar_job.py
    upload_stats.py
    system_perf_app.py
    table_size.py
    tpc_worker.py
    tuneCgroups.py
    tuneRuntime.py
    downloadAndRunApp.sh
    grpc_server_app.py
    controller_app.py
    csvtoparquet_nopandas.py
    notebook_app.py
)

add_custom_target(LINKED_SCRIPTS ALL
    DEPENDS ${LOCAL_LINKED_SCRIPTS} GENERATED_PYTHON_PROTOBUF)

add_custom_command(
    OUTPUT SetXceDefaults.sh
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/fileBuild.sh ${CMAKE_SOURCE_DIR}/conf/default.conf ${CMAKE_CURRENT_SOURCE_DIR}/SetXceDefaults.sh.tmpl > SetXceDefaults.sh
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/fileBuild.sh ${CMAKE_CURRENT_SOURCE_DIR}/SetXceDefaults.sh.tmpl
    )

add_custom_target(setXceDefaults ALL
    DEPENDS SetXceDefaults.sh)

add_dependencies(installedObjects
    setXceDefaults
    LINKED_SCRIPTS)

install(
    FILES
        ${STATIC_SCRIPTS}
        ${CMAKE_CURRENT_BINARY_DIR}/SetXceDefaults.sh
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
                GROUP_EXECUTE GROUP_WRITE GROUP_READ
                WORLD_EXECUTE             WORLD_READ
    DESTINATION
        "scripts")
install(DIRECTORY scheduleRetinas DESTINATION "scripts"
    FILES_MATCHING
        PATTERN "*.py"
        PATTERN "*.txt")

install(DIRECTORY performance DESTINATION "scripts"
    FILES_MATCHING
        PATTERN "*.py"
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
                GROUP_EXECUTE GROUP_WRITE GROUP_READ
                WORLD_EXECUTE             WORLD_READ)

install(DIRECTORY test_loadqa  DESTINATION "scripts"
    FILES_MATCHING
        PATTERN "*.py"
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
                GROUP_EXECUTE GROUP_WRITE GROUP_READ
                WORLD_EXECUTE             WORLD_READ)

install(DIRECTORY exampleWorkbooks DESTINATION "scripts")
