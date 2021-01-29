# Protobuf related build infrastructure

find_package(Protobuf REQUIRED)

set(PROTO_SRC_ROOT "${CMAKE_SOURCE_DIR}/src/include/pb")

set(PROTO_DST_ROOT "${CMAKE_BINARY_DIR}/src/include/pb")

set(PROTO_DIR "${PROTO_DST_ROOT}/xcalar/compute/localtypes")

add_custom_command(
    OUTPUT ${PROTO_DIR}
    COMMAND
        ${CMAKE_COMMAND} -E  make_directory ${PROTO_DIR}
)

function(XC_PROTOBUF_LINK_TO_DST)
    set(multiValueArgs DEPENDS FILES LINKED_FILES_RESULT)
    cmake_parse_arguments(ARG
            ""
            ""
            "${multiValueArgs}"
            ${ARGN})
    if(NOT "${ARG_UNPARSED_ARGUMENTS}" STREQUAL "")
        message(SEND_ERROR "Unrecognized arguments '${ARG_UNPARSED_ARGUMENTS}'")
    endif()
    set(_linked_files "")
    foreach (protoSource ${ARG_FILES})
        get_filename_component(protoName ${protoSource} NAME_WE)
        add_custom_command(
                OUTPUT "${PROTO_DIR}/${protoName}.proto"
                COMMAND ${CMAKE_COMMAND} -E create_symlink
                ${protoSource} "${PROTO_DIR}/${protoName}.proto"
                DEPENDS ${protoSource} ${ARG_DEPENDS} ${PROTO_DIR}
        )
        list(APPEND _linked_files "${PROTO_DIR}/${protoName}.proto")
    endforeach (protoSource)
    set(${ARG_LINKED_FILES_RESULT} ${_linked_files} PARENT_SCOPE)
endfunction()
# XC_PROTOBUF_GENERATE
#   Generate source code from .proto files
#
# Arguments:
#   PROTO_PATH      - Top path from which to compute the full name of
#                     protoFiles. Protobuf files are all namespaced and this
#                     must be respected; mylib/hello.proto is different from
#                     hello.proto.
#                     XXX Currently only a single path is respected.
#   PROTOC_FLAGS    - Additional flags to protoc
#   DEPENDS         - Additional dependencies which must be met before running
#                     the protoc code generation
#   FILES           - .proto files from which to generate the sources. Should
#                     be a subdir under PROTO_PATH.
#   CPP_OUT         - Output directory for c++ files
#   PY_OUT          - Output directory for python files
#   CPP_SERVICE_OUT - Output directory for c++ service files
#   JS_OUT          - Output directory for js files
#
# Outputs:
#   CPP_HDRS        - Output variable for c++ .h files
#   CPP_SRCS        - Output variable for c++ .cc files
#   PY_SRCS         - Output variable for python files
#   CPP_SERVICE_HDRS- Output variable for c++ service .h files
#   CPP_SERVICE_SRCS- Output variable for c++ service .cpp files
#   JS_SERVICE_CLIENT_SRCS- Output variable for js client stubs files
#   JS_SERVICE_SERVER_SRCS- Output variable for js service information files

function(XC_PROTOBUF_GENERATE)
    set(singleValueArgs
        PROTOC_FLAGS
        PROTO_PATH
        CPP_HDRS CPP_SRCS CPP_OUT
        CPP_SERVICE_HDRS CPP_SERVICE_SRCS CPP_SERVICE_OUT
        PY_SRCS PY_OUT
        PY_SERVICE_SRCS PY_SERVICE_OUT
        JS_SRCS JS_OUT
        JS_SERVICE_CLIENT_SRCS JS_SERVICE_SERVER_SRCS JS_SERVICE_OUT
        )
    set(multiValueArgs DEPENDS FILES)
    cmake_parse_arguments(ARG
        ""
        "${singleValueArgs}"
        "${multiValueArgs}"
        ${ARGN})
    if(NOT "${ARG_UNPARSED_ARGUMENTS}" STREQUAL "")
        message(SEND_ERROR "Unrecognized arguments '${ARG_UNPARSED_ARGUMENTS}'")
    endif()

    set(_cppHdrs "")
    set(_cppSrcs "")
    set(_pySrcs "")
    set(_cppServiceHdrs "")
    set(_cppServiceSrcs "")
    set(_jsSrcs "")
    set(_jsServiceSrcs "")
    set(_jsServiceInfoSrcs "")
    set(_pyServiceSrcs "")

    set(cppDir ${CMAKE_CURRENT_BINARY_DIR})
    set(pyDir ${CMAKE_CURRENT_BINARY_DIR})
    set(jsDir ${CMAKE_CURRENT_BINARY_DIR})
    set(cppServiceDir ${CMAKE_CURRENT_BINARY_DIR})
    set(jsServiceDir ${CMAKE_CURRENT_BINARY_DIR})
    set(pyServiceDir ${CMAKE_CURRENT_BINARY_DIR})

    set(protoFiles ${ARG_FILES})

    if(NOT "${ARG_CPP_OUT}" STREQUAL "")
        set(cppDir ${ARG_CPP_OUT})
    endif()

    if(NOT "${ARG_PY_OUT}" STREQUAL "")
        set(pyDir ${ARG_PY_OUT})
    endif()

    if(NOT "${ARG_CPP_SERVICE_OUT}" STREQUAL "")
        set(cppServiceDir ${ARG_CPP_SERVICE_OUT})
    endif()

    if(NOT "${ARG_JS_OUT}" STREQUAL "")
        set(jsDir ${ARG_JS_OUT})
    endif()

    if(NOT "${ARG_JS_SERVICE_OUT}" STREQUAL "")
        set(jsServiceDir ${ARG_JS_SERVICE_OUT})
    endif()

    if(NOT "${ARG_PY_SERVICE_OUT}" STREQUAL "")
        set(pyServiceDir ${ARG_PY_SERVICE_OUT})
    endif()

    foreach(protoFile ${protoFiles})
        get_filename_component(protoBase ${protoFile} NAME_WE)
        get_filename_component(protoDir ${protoFile} DIRECTORY)
        file(RELATIVE_PATH nameSpace ${ARG_PROTO_PATH} ${protoDir})

        if("${nameSpace}" STREQUAL "")
            set(nameSpace ".")
        endif()

        if(NOT "${ARG_CPP_SRCS}" STREQUAL "")
            set(cppFile "${cppDir}/${nameSpace}/${protoBase}.pb.cc")
            set(hFile "${cppDir}/${nameSpace}/${protoBase}.pb.h")
            set(grpcCppFile "${cppDir}/${nameSpace}/${protoBase}.grpc.pb.cc")
            set(grpcHFile "${cppDir}/${nameSpace}/${protoBase}.grpc.pb.h")
            add_custom_command(
                OUTPUT
                    ${cppFile} ${hFile} ${grpcCppFile} ${grpcHFile}
                COMMAND
                    ${CMAKE_COMMAND} -E make_directory "${cppDir}"
                COMMAND
                    ${PROTOBUF_PROTOC_EXECUTABLE}
                        --grpc_out=${cppDir}
                        --cpp_out=${cppDir}
                        --proto_path=${ARG_PROTO_PATH}
                        --plugin=protoc-gen-grpc="${CMAKE_INSTALL_PREFIX}/bin/grpc_cpp_plugin"
                        ${ARG_PROTOC_FLAGS}
                        ${protoFile}
                DEPENDS
                    ${protoFile} ${ARG_DEPENDS}
                )
            list(APPEND _cppSrcs ${cppFile})
            list(APPEND _cppHdrs ${hFile})
            list(APPEND _cppSrcs ${grpcCppFile})
            list(APPEND _cppHdrs ${grpcHFile})
        endif()

        if(NOT "${ARG_PY_SRCS}" STREQUAL "")
            set(pyFile "${pyDir}/${nameSpace}/${protoBase}_pb2.py")
            set(grpcPyFile "${pyDir}/${nameSpace}/${protoBase}_grpc.py")
            add_custom_command(
                OUTPUT
                    ${pyFile} ${grpcPyFile}
                COMMAND
                    ${CMAKE_COMMAND} -E make_directory "${pyDir}"
                COMMAND
                    ${PROTOBUF_PROTOC_EXECUTABLE}
                        --python_out=${pyDir}
                        --grpc_python_out=${pyDir}
                        --proto_path=${ARG_PROTO_PATH}
                        --plugin=protoc-gen-grpc_python="${CMAKE_INSTALL_PREFIX}/bin/grpc_python_plugin"
                        ${ARG_PROTOC_FLAGS}
                        ${protoFile}
                DEPENDS
                    ${protoFile} ${ARG_DEPENDS}
                )
            list(APPEND _pySrcs ${pyFile} ${grpcPyFile})
        endif()

        if(NOT "${ARG_JS_SRCS}" STREQUAL "")
            set(jsFile "${jsDir}/${nameSpace}/${protoBase}_pb.js")
            add_custom_command(
                OUTPUT
                    ${jsFile}
                COMMAND
                    ${CMAKE_COMMAND} -E make_directory "${jsDir}"
                COMMAND
                    ${PROTOBUF_PROTOC_EXECUTABLE}
                        --js_out=import_style=commonjs,binary:${jsDir}
                        --proto_path=${ARG_PROTO_PATH}
                        ${ARG_PROTOC_FLAGS}
                        ${protoFile}
                DEPENDS
                    ${protoFile} ${ARG_DEPENDS}
                )
            list(APPEND _jsSrcs ${jsFile})
        endif()

        if(NOT "${ARG_JS_SRCS}" STREQUAL "")
            set(jsGrpcFile "${jsDir}/${nameSpace}/${protoBase}_grpc_pb.js")
            add_custom_command(
                OUTPUT
                    ${jsGrpcFile}
                COMMAND
                    ${CMAKE_COMMAND} -E make_directory "${jsDir}"
                COMMAND
                    ${PROTOBUF_PROTOC_EXECUTABLE}
                        --js_out=import_style=commonjs,binary:${jsDir}
                        --grpc_out=${jsDir}
                        --plugin=protoc-gen-grpc="${CMAKE_INSTALL_PREFIX}/bin/grpc_node_plugin"
                        --proto_path=${ARG_PROTO_PATH}
                        ${ARG_PROTOC_FLAGS}
                        ${protoFile}
                DEPENDS
                    ${protoFile} ${ARG_DEPENDS}
                )
            list(APPEND _jsSrcs ${jsGrpcFile})
        endif()

#
# This is needed for grpc web access someday but for some reason it doesn't work in a rel7 
# container.  It works on ub18 native.  Need to debug this when we need grpc web access.
#
#       if(NOT "${ARG_JS_SRCS}" STREQUAL "")
#           set(jsWebFile "${jsDir}/${nameSpace}/${protoBase}_grpc_web_pb.js")
#           add_custom_command(
#               OUTPUT
#                   ${jsWebFile}
#               COMMAND
#                   ${CMAKE_COMMAND} -E make_directory "${jsDir}"
#               COMMAND
#                   ${PROTOBUF_PROTOC_EXECUTABLE}
#                       --grpc-web_out=import_style=commonjs,mode=grpcwebtext:${jsDir}
#                       --proto_path=${ARG_PROTO_PATH}
#                       ${ARG_PROTOC_FLAGS}
#                       ${protoFile}
#               DEPENDS
#                   ${protoFile} ${ARG_DEPENDS}
#               )
#           list(APPEND _jsSrcs ${jsWebFile})
#       endif()

        if(NOT "${ARG_JS_SERVICE_CLIENT_SRCS}" STREQUAL "")
            set(jsServiceFile "${jsDir}/${protoBase}_xcrpc.js")
            add_custom_command(
                OUTPUT
                    ${jsServiceFile}
                COMMAND
                    ${CMAKE_COMMAND} -E make_directory "${jsServiceDir}"
                COMMAND
                    ${PROTOBUF_PROTOC_EXECUTABLE}
                        --xcrpc_js_out=${jsServiceDir}
                        --proto_path=${ARG_PROTO_PATH}
                        ${ARG_PROTOC_FLAGS}
                        ${protoFile}
                DEPENDS
                    ${protoFile} ${ARG_DEPENDS}
                    ${CMAKE_SOURCE_DIR}/bin/protoc-gen-xcrpc_js
                    ${CMAKE_SOURCE_DIR}/src/bin/xcrpc/Service_xcrpc.js.j2
                )
            list(APPEND _jsServiceSrcs ${jsServiceFile})
        endif()

        if(NOT "${ARG_JS_SERVICE_SERVER_SRCS}" STREQUAL "")
            set(jsServiceInfoFile "${jsDir}/${protoBase}_serviceInfo.js")
            add_custom_command(
                OUTPUT
                    ${jsServiceInfoFile}
                COMMAND
                    ${CMAKE_COMMAND} -E make_directory "${jsServiceDir}"
                COMMAND
                    ${PROTOBUF_PROTOC_EXECUTABLE}
                        --xcrpc-serviceinfos_js_out=${jsServiceDir}
                        --proto_path=${ARG_PROTO_PATH}
                        ${ARG_PROTOC_FLAGS}
                        ${protoFile}
                DEPENDS
                    ${protoFile} ${ARG_DEPENDS}
                    ${CMAKE_SOURCE_DIR}/bin/protoc-gen-xcrpc-serviceinfos_js
                    ${CMAKE_SOURCE_DIR}/src/bin/xcrpc/Service_info.js.j2
                )
            list(APPEND _jsServiceInfoSrcs ${jsServiceInfoFile})
        endif()

        if(NOT "${ARG_PY_SERVICE_SRCS}" STREQUAL "")
            set(pyServiceFile "${pyServiceDir}/${protoBase}_xcrpc.py")
            add_custom_command(
                OUTPUT
                    ${pyServiceFile}
                COMMAND
                    ${CMAKE_COMMAND} -E make_directory "${pyServiceDir}"
                COMMAND
                    ${PROTOBUF_PROTOC_EXECUTABLE}
                        --xcrpc_py_out=${pyServiceDir}
                        --proto_path=${ARG_PROTO_PATH}
                        ${ARG_PROTOC_FLAGS}
                        ${protoFile}
                DEPENDS
                    ${protoFile} ${ARG_DEPENDS}
                    ${CMAKE_SOURCE_DIR}/bin/protoc-gen-xcrpc_py
                    ${CMAKE_SOURCE_DIR}/src/bin/xcrpc/Service_xcrpc.py.j2
                )
            list(APPEND _pyServiceSrcs ${pyServiceFile})
        endif()

        if(NOT "${ARG_CPP_SERVICE_SRCS}" STREQUAL "")
            set(cppServiceHdr "${cppServiceDir}/${nameSpace}/${protoBase}.xcrpc.h")
            set(cppServiceSrc "${cppServiceDir}/${nameSpace}/${protoBase}.xcrpc.cpp")
            add_custom_command(
                OUTPUT
                    ${cppServiceHdr} ${cppServiceSrc}
                COMMAND
                    ${CMAKE_COMMAND} -E make_directory "${cppServiceDir}"
                COMMAND
                    ${PROTOBUF_PROTOC_EXECUTABLE}
                        --xcrpc_cpp_out=${cppServiceDir}
                        --proto_path=${ARG_PROTO_PATH}
                        ${ARG_PROTOC_FLAGS}
                        ${protoFile}
                DEPENDS
                    ${protoFile} ${ARG_DEPENDS}
                    ${CMAKE_SOURCE_DIR}/bin/protoc-gen-xcrpc_cpp
                    ${CMAKE_SOURCE_DIR}/src/bin/xcrpc/Service.xcrpc.h.j2
                    ${CMAKE_SOURCE_DIR}/src/bin/xcrpc/Service.xcrpc.cpp.j2
                )
            list(APPEND _cppServiceSrcs ${cppServiceSrc})
            list(APPEND _cppServiceHdrs ${cppServiceHdr})
        endif()
    endforeach()

    # Set all of our potential return values
    if(NOT "${ARG_CPP_HDRS}" STREQUAL "")
        set(${ARG_CPP_HDRS} ${_cppHdrs} PARENT_SCOPE)
    endif()
    if(NOT "${ARG_CPP_SRCS}" STREQUAL "")
        set(${ARG_CPP_SRCS} ${_cppSrcs} PARENT_SCOPE)
    endif()
    if(NOT "${ARG_PY_SRCS}" STREQUAL "")
        set(${ARG_PY_SRCS} ${_pySrcs} PARENT_SCOPE)
    endif()
    if(NOT "${ARG_JS_SRCS}" STREQUAL "")
        set(${ARG_JS_SRCS} ${_jsSrcs} PARENT_SCOPE)
    endif()
    if(NOT "${ARG_JS_SERVICE_CLIENT_SRCS}" STREQUAL "")
        set(${ARG_JS_SERVICE_CLIENT_SRCS} ${_jsServiceSrcs} PARENT_SCOPE)
    endif()
    if(NOT "${ARG_JS_SERVICE_SERVER_SRCS}" STREQUAL "")
        set(${ARG_JS_SERVICE_SERVER_SRCS} ${_jsServiceInfoSrcs} PARENT_SCOPE)
    endif()
    if(NOT "${ARG_PY_SERVICE_SRCS}" STREQUAL "")
        set(${ARG_PY_SERVICE_SRCS} ${_pyServiceSrcs} PARENT_SCOPE)
    endif()
    if(NOT "${ARG_CPP_SERVICE_HDRS}" STREQUAL "")
        set(${ARG_CPP_SERVICE_HDRS} ${_cppServiceHdrs} PARENT_SCOPE)
    endif()
    if(NOT "${ARG_CPP_SERVICE_SRCS}" STREQUAL "")
        set(${ARG_CPP_SERVICE_SRCS} ${_cppServiceSrcs} PARENT_SCOPE)
    endif()
endfunction()
