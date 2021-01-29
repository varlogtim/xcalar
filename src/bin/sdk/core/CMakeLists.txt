include(genProto)
# Thrift pulling
set(THRIFT_PY_LOCAL "xcalar/compute/coretypes")
set(THRIFT_PY_SOURCE
    "${CMAKE_BINARY_DIR}/src/bin/thrift/py")
set(SERVICE_INIT
    "${CMAKE_CURRENT_BINARY_DIR}/xcalar/compute/services/__init__.py")

xc_protobuf_generate(
    PY_SERVICE_SRCS
        pyServiceSrcs
    PY_SERVICE_OUT
        "${CMAKE_CURRENT_BINARY_DIR}/xcalar/compute/services"
    PY_SRCS
        pyDataSrcs
    PY_OUT
        "${CMAKE_CURRENT_BINARY_DIR}"
    PROTO_PATH
        "${PROTO_DST_ROOT}"
    FILES
        ${PROTO_SERVICE_FILES}
    )

add_custom_command(
    OUTPUT
        ${SERVICE_INIT}
    COMMAND
        ${CMAKE_COMMAND} -E make_directory
            "${CMAKE_CURRENT_BINARY_DIR}/xcalar/compute/services"
    COMMAND ${CMAKE_COMMAND} -E touch "${SERVICE_INIT}"
    )

add_custom_command(
    OUTPUT
        ${THRIFT_PY_LOCAL}
    COMMAND
        ${CMAKE_COMMAND} -E copy
            ${CMAKE_CURRENT_SOURCE_DIR}/namespace_init.py
            "xcalar/__init__.py"
    COMMAND
        ${CMAKE_COMMAND} -E copy
            ${CMAKE_CURRENT_SOURCE_DIR}/namespace_init.py
            "xcalar/compute/__init__.py"
    COMMAND
        ${CMAKE_COMMAND} -E create_symlink
            ${THRIFT_PY_SOURCE} ${THRIFT_PY_LOCAL}
    DEPENDS
        thriftPyFiles # This is the name of the generated files target
    COMMENT
        "Assembling python library xcalar.compute.core"
    )


# Protobuf pulling
set(PROTO_SCRIPT_SOURCES
    ${CMAKE_BINARY_DIR}/src/lib/libprotobuf/xcalar/compute/localtypes/ProtoMsg_pb2.py
    ${CMAKE_BINARY_DIR}/src/lib/libprotobuf/xcalar/compute/localtypes/ParentChild_pb2.py
    ${CMAKE_BINARY_DIR}/src/lib/libprotobuf/xcalar/compute/localtypes/ProtoFieldValue_pb2.py
)

set(PROTO_PY_LOCAL "")
foreach(scriptSource ${PROTO_SCRIPT_SOURCES})
    get_filename_component(localFile ${scriptSource} NAME)
    set(protoDest "${CMAKE_CURRENT_BINARY_DIR}/xcalar/compute/localtypes/${localFile}")
    add_custom_command(
        OUTPUT ${protoDest}
        COMMAND ${CMAKE_COMMAND} -E make_directory "xcalar/compute/localtypes"
        COMMAND ${CMAKE_COMMAND} -E touch "xcalar/compute/localtypes/__init__.py"
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${scriptSource} ${protoDest}
        DEPENDS GENERATED_PYTHON_PROTOBUF)
    list(APPEND PROTO_PY_LOCAL ${protoDest})
endforeach(scriptSource)

# Python packaging
set(LINKED_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/setup.py
    ${CMAKE_CURRENT_SOURCE_DIR}/README.rst
)
set(LOCAL_FILES "")
foreach(sourceFile ${LINKED_FILES})
    get_filename_component(fileName ${sourceFile} NAME)
    set(localFile ${CMAKE_CURRENT_BINARY_DIR}/${fileName})
    add_custom_command(
        OUTPUT ${localFile}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${sourceFile} ${localFile}
        DEPENDS ${sourceFile})
    list(APPEND LOCAL_FILES ${localFile})
endforeach(sourceFile)
add_custom_target(coreFiles
    DEPENDS
        ${THRIFT_PY_LOCAL}
        ${SERVICE_INIT}
        ${pyServiceSrcs}
        ${pyDataSrcs}
        ${PROTO_PY_LOCAL}
        ${LOCAL_FILES})

add_dependencies(xceBins coreFiles)

# XXX there's some wonky stuff going on here with the INSTALL_DEPENDS.
# See src/bin/sdk/xdp/CMakeLists.txt for more info
add_xce_py_package(pyCorePackage
    CODE_DIR ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS coreFiles
    INSTALL_TARGET xceCoreInstall
    INSTALL_DEPENDS xcThriftInstall)
