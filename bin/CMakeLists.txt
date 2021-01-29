set(LINKED_BINARIES
    ${CMAKE_BINARY_DIR}/src/bin/usrnode/usrnode
    ${CMAKE_BINARY_DIR}/src/bin/childnode/childnode
    ${CMAKE_BINARY_DIR}/src/bin/cli/xccli
    ${CMAKE_BINARY_DIR}/src/bin/mgmtd/xcmgmtd
    ${CMAKE_BINARY_DIR}/src/bin/monitor/xcmonitor
    ${CMAKE_BINARY_DIR}/src/misc/xcMapRClient/xcMapRClient
    ${CMAKE_BINARY_DIR}/src/misc/xcUpgradeTool/xcUpgradeTool
    ${CMAKE_BINARY_DIR}/src/bin/licenseTools/licenseCheck
    ${CMAKE_BINARY_DIR}/src/bin/licenseTools/signKey
    ${CMAKE_BINARY_DIR}/src/bin/licenseTools/readKey
    ${CMAKE_SOURCE_DIR}/src/bin/licenseTools/CreateKey.py
    ${CMAKE_SOURCE_DIR}/src/bin/xcrpc/protoc-gen-xcrpc_cpp
    ${CMAKE_SOURCE_DIR}/src/bin/xcrpc/protoc-gen-xcrpc_js
    ${CMAKE_SOURCE_DIR}/src/bin/xcrpc/protoc-gen-xcrpc-serviceinfos_js
    ${CMAKE_SOURCE_DIR}/src/bin/xcrpc/protoc-gen-xcrpc_py
    ${CMAKE_BINARY_DIR}/src/bin/legacyJsClient/jsPackage
    )

set(LINKED_QA_BINARIES
    ${CMAKE_BINARY_DIR}/src/3rd/dbgen/dbgen
    )

set(LOCAL_BINARIES "")
foreach(binarySource ${LINKED_BINARIES})
    get_filename_component(localFile ${binarySource} NAME)
    set(inSourceFile "${CMAKE_CURRENT_SOURCE_DIR}/${localFile}")
    # Note that this is modifying the source directory, NOT the build directory
    add_custom_command(
        OUTPUT ${inSourceFile}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${binarySource} ${inSourceFile})
    list(APPEND LOCAL_BINARIES ${inSourceFile})
endforeach(binarySource)

set(LOCAL_QA_BINARIES "")
foreach(binarySource ${LINKED_QA_BINARIES})
    get_filename_component(localFile ${binarySource} NAME)
    set(inSourceFile "${CMAKE_CURRENT_SOURCE_DIR}/${localFile}")
    # Note that this is modifying the source directory, NOT the build directory
    add_custom_command(
        OUTPUT ${inSourceFile}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${binarySource} ${inSourceFile})
    list(APPEND LOCAL_QA_BINARIES ${inSourceFile})
endforeach(binarySource)

add_custom_target(LINKED_BINARIES ALL
    DEPENDS ${LOCAL_BINARIES})
add_dependencies(xceBins LINKED_BINARIES)

add_custom_target(LINKED_QA_BINARIES ALL
    DEPENDS ${LOCAL_QA_BINARIES})
add_dependencies(qa LINKED_QA_BINARIES)

add_custom_target(BUILD_SHA ALL
    COMMAND	${CMAKE_SOURCE_DIR}/bin/genBuildSha.sh > ${CMAKE_CURRENT_BINARY_DIR}/BUILD_SHA)
add_dependencies(installedObjects BUILD_SHA)

install(FILES xcalarctl xcalar-start-pre.sh cgconfig-setup.sh osid
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                    GROUP_READ GROUP_WRITE GROUP_EXECUTE
                    WORLD_READ             WORLD_EXECUTE
        DESTINATION bin)
install(FILES asup-service.sh caddy-service.sh expserver-service.sh jupyter-service.sh
              sqldf-service.sh usrnode-service.sh usrnode-service-responding.sh
              terminal-service.sh xcmgmtd-service.sh java_home.sh
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                    GROUP_READ GROUP_WRITE GROUP_EXECUTE
                    WORLD_READ             WORLD_EXECUTE
        DESTINATION bin)
install(FILES determineNodeId.sh cgroupMgmtUtils.sh
        PERMISSIONS OWNER_READ OWNER_WRITE
                    GROUP_READ
                    WORLD_READ
        DESTINATION bin)
install(FILES cgroupControllerUtil.sh
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
        DESTINATION bin)
install(FILES genConfig.sh
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                    GROUP_READ GROUP_WRITE GROUP_EXECUTE
                    WORLD_READ             WORLD_EXECUTE
        DESTINATION scripts)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/BUILD_SHA" DESTINATION share/doc/xcalar-${XLRVERSION})
