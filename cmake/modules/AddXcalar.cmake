# Adds a library to XCE
macro(add_xce_library name)
    cmake_parse_arguments(ARG
        ""
        ""
        ""
        ${ARGN})
    xc_add_library(${name} ${ARG_UNPARSED_ARGUMENTS})

    # Must be built after headers, since they must be included
    add_dependencies(${name} GENERATED_HEADERS)
endmacro(add_xce_library name)

macro(add_xce_executable name)
    cmake_parse_arguments(ARG
        ""
        ""
        ""
        ${ARGN})
    xc_add_executable(${name} ${ARG_UNPARSED_ARGUMENTS})

    add_dependencies(xceBins ${name})
endmacro(add_xce_executable name)

macro(add_xce_test_executable name)
    cmake_parse_arguments(ARG
        ""
        ""
        ""
        ${ARGN})
    xc_add_executable(${name} ${ARG_UNPARSED_ARGUMENTS})

    add_dependencies(qa ${name})
endmacro(add_xce_test_executable name)

# Adds a library, performing additional management of the source files.
# Only use this directly for C++ libraries OUTSIDE of XCE.
#  xc_add_library(name sources...
#    SHARED;STATIC
#      STATIC build as a static library
#      SHARED build as a shared library
#    GENERATED generatedSources...
#      All sources which are generated and thus shouldn't be style checked etc.
#  )
function(xc_add_library name)
    cmake_parse_arguments(ARG
        "STATIC;SHARED"
        ""
        ""
        ${ARGN})
    set(ALL_FILES ${ARG_UNPARSED_ARGUMENTS})
    if(ARG_STATIC AND ARG_SHARED)
        message(FATAL_ERROR "STATIC and SHARED doesn't make sense")
    endif()

    set(LIB_TYPE STATIC) # Default is static
    if(ARG_SHARED)
        set(LIB_TYPE SHARED)
    endif()

    set(formatTarget ${name}_clangformat)
    add_custom_target(${formatTarget})

    xc_process_files(ALL_FILES
        ${ARG_UNPARSED_ARGUMENTS})

    add_library(${name} ${LIB_TYPE} ${ALL_FILES})
    add_dependencies(${name} ${formatTarget})
endfunction(xc_add_library name)

# Adds an executable, performing additional management of the source files.
# Only use this directly for C++ libraries OUTSIDE of XCE.
#  xc_add_executable(name sources...
#    GENERATED generatedSources...
#      All sources which are generated and thus shouldn't be style checked etc.
#  )
function(xc_add_executable name)
    cmake_parse_arguments(ARG
        ""
        ""
        ""
        ${ARGN})

    set(formatTarget ${name}_clangformat)
    add_custom_target(${formatTarget})

    xc_process_files(ALL_FILES
        ${ARG_UNPARSED_ARGUMENTS})

    add_executable(${name} ${ALL_FILES})
endfunction(xc_add_executable name)

function(xc_process_files ALL_FILES_OUT)
    cmake_parse_arguments(ARG
        ""
        ""
        "GENERATED"
        ${ARGN})
    set(WRITTEN_FILES ${ARG_UNPARSED_ARGUMENTS})
    set(GENERATED_FILES ${ARG_GENERATED})
    set(ALL_FILES ${WRITTEN_FILES} ${GENERATED_FILES})

    foreach(file ${WRITTEN_FILES})
        if(NOT IS_ABSOLUTE ${file})
            set(file ${CMAKE_CURRENT_SOURCE_DIR}/${file})
        endif()

        file(RELATIVE_PATH rel ${CMAKE_BINARY_DIR} ${file})
        string(FIND ${rel} ".." notSource)

        string(FIND ${file} "3rd" file3rd)

        string(FIND ${file} ".pb.cc" protoCC)
        string(FIND ${file} ".pb.h" protoH)
        if(${file3rd} EQUAL -1 AND
           ${protoCC} EQUAL -1 AND
           ${protoH} EQUAL -1 AND
           NOT ${notSource} EQUAL -1)
            xc_format_file(${file})
        endif()
    endforeach(file)
    set(${ALL_FILES_OUT} ${ALL_FILES} PARENT_SCOPE)
endfunction(xc_process_files)

set(styleIgnore
    "Bitmap.cpp"
    "GetOpt.c"
    "DagDurable.cpp"
    "DagDurable.h"
    "DagDurableMinorVers.cpp"
    "KvStoreDurable.cpp"
    "KvStoreDurable.h"
    "KvStoreDurableMinorVers.cpp"
    "DataTargetDurable.cpp"
    "DataTargetDurable.h"
    "DataTargetDurableMinorVers.cpp"
    "SessionDurable.cpp"
    "SessionDurable.h"
    "SessionDurableMinorVers.cpp"
    "DurableVersions.h"
    "midl.c"
    "midl.h"
    "mdb.c"
    "lmdb.h"
    "bitmapBenchmarks.cpp"
)

set(bannedSymbolsIgnore
    "MemTrack.h"
    "MemTrackStress.cpp"
    "FaultHandler.c"
    "BoyerMoore.c"
    "connection.cpp"
    "monitor.cpp"
    "RandDag.cpp"
    "AppFuncTests.h"
    "lmdb.h"
    "mdb.c"
    "midl.c"
    "midl.h"
)

function(xc_format_file file)
    get_filename_component(fileBase ${file} NAME)
    string(MD5 fileHash ${file})
    set(cstyleTarget "cstyle_${fileBase}")
    set(bannedSymbolsTarget "bannedSymbols_${fileBase}")
    set(fileFormatTarget "${fileHash}_${fileBase}_clang_format")
    set(formatValidationTarget "${fileHash}_${fileBase}_clang_format_valid")

    # Add cstyle target if it's not ignored
    list(FIND styleIgnore ${fileBase} _styleIgnore)
    if(${_styleIgnore} EQUAL -1)
        if(NOT TARGET ${cstyleTarget})
            add_custom_target(${cstyleTarget}
                COMMAND
                    cstyle.pl ${file})
            add_dependencies(cstyle ${cstyleTarget})
        endif()
    endif()

    # Add bannedSymbolsCheck target if it's not ignored
    list(FIND bannedSymbolsIgnore ${fileBase} _index)
    if(${_index} EQUAL -1)
        if(NOT TARGET ${cstyleTarget})
            add_custom_target(${bannedSymbolsTarget}
                COMMAND
                    bannedSymbolsCheck.sh ${file})
            add_dependencies(bannedSymbolsCheck ${bannedSymbolsTarget})
        endif()
    endif()

    # format the code at user's request

    if(${_styleIgnore} EQUAL -1 AND
       NOT TARGET ${fileFormatTarget})
        add_custom_target(${fileFormatTarget}
            COMMAND
                clang-format -i ${file}
            COMMENT
                "formating ${file}"
            )
        add_dependencies(clangFormat ${fileFormatTarget})
    endif()

    # validate code format
    if(${_styleIgnore} EQUAL -1 AND
       NOT TARGET ${formatValidationTarget})
        add_custom_target(${formatValidationTarget}
            COMMAND
                clang-format ${file} | diff -q ${file} /dev/fd/0
            COMMENT
                "Checking clang-format compliance of ${file}"
            DEPENDS
                ${file})
        add_dependencies(clangFormatVerify ${formatValidationTarget})
    endif()
endfunction(xc_format_file)

macro(add_xce_py_package target)
    set(singleValueArgs CODE_DIR INSTALL_TARGET)
    set(multiValueArgs DEPENDS INSTALL_DEPENDS)
    cmake_parse_arguments(ARG
        ""
        "${singleValueArgs}"
        "${multiValueArgs}"
        ${ARGN})

    set(installStamp ${CMAKE_CURRENT_BINARY_DIR}/dev_install.stamp)
    add_custom_target(${ARG_INSTALL_TARGET}
        DEPENDS ${installStamp}
        )

    # At build time we're going to do a 'dev install' of
    # our packages. This should only need to be done once,
    # so we're going to keep a stamp around to remember
    add_custom_command(
        OUTPUT
            ${installStamp}
        COMMAND
            pip3 install -q -e . # -q for quiet, -e for dev
        COMMAND
            touch ${installStamp}
        WORKING_DIRECTORY
            ${ARG_CODE_DIR}
        DEPENDS
            ${ARG_INSTALL_DEPENDS} ${ARG_DEPENDS}
        COMMENT
            "Installing dev build of ${target}"
        )

    # 'Install' the package directly into the installation directory
    set(packageDir "${CMAKE_CURRENT_BINARY_DIR}/built")
    set(stamp "${CMAKE_CURRENT_BINARY_DIR}/built.stamp")
    add_custom_target(${target}
        DEPENDS ${stamp})
    add_dependencies(sdkPackage ${target})
    add_custom_command(
        OUTPUT
            ${stamp}
        COMMAND
            # We only specify the build command to control the build directory;
            # 'install' implies 'build' typically
            # https://bugs.python.org/issue1011113
            python3.6 setup.py -q build -b ${CMAKE_CURRENT_BINARY_DIR}/build install --prefix ${PREFIX} --root "${packageDir}"
        COMMAND
            touch ${stamp}
        WORKING_DIRECTORY
            ${ARG_CODE_DIR}
        DEPENDS
            ${ARG_DEPENDS}
        COMMENT
            "Building install dir for ${target}"
    )
    install(DIRECTORY ${packageDir}/opt DESTINATION "/"
            PATTERN "xcalar/bin/xc2"
            PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
                        GROUP_EXECUTE GROUP_READ
                        WORLD_EXECUTE WORLD_READ)

    # Create a wheel to be installed elsewhere
    set(wheelTarget ${target}_wheel)
    set(wheelStamp "${CMAKE_CURRENT_BINARY_DIR}/wheel.stamp")
    set(distDir ${CMAKE_CURRENT_BINARY_DIR}/dist)
    add_custom_target(${wheelTarget}
        DEPENDS ${wheelStamp})
    add_dependencies(sdkPackage ${wheelTarget})
    add_custom_command(
        OUTPUT
            ${wheelStamp}
        COMMAND
            # We only specify the build command to control the build directory;
            # 'install' implies 'build' typically
            # https://bugs.python.org/issue1011113
            python3.6 setup.py -q build -b ${CMAKE_CURRENT_BINARY_DIR}/build bdist_wheel --dist-dir ${distDir}
        COMMAND
            touch ${wheelStamp}
        WORKING_DIRECTORY
            ${ARG_CODE_DIR}
        COMMENT
            "Building wheel for ${target}"
        # NOTE: here we are specifying that this wheel target 'depends' on the
        # above tree-based-install target. These do NOT depend on each other,
        # but must never run at the same time. Both of these commands will create
        # an egg before doing their primary function. If they run at the same
        # time, this egg directory might get trampled on mid-use.
        DEPENDS ${ARG_DEPENDS} ${target}
    )
endmacro(add_xce_py_package target)
