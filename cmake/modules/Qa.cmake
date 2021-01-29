macro(link_qa_file filename)
    if(IS_ABSOLUTE ${filename})
        set(sourceFn ${filename})
    else()
        set(sourceFn ${CMAKE_CURRENT_SOURCE_DIR}/${filename})
    endif()

    add_custom_command(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${filename}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${sourceFn}
            ${CMAKE_CURRENT_BINARY_DIR}/${filename}
    )
    string(MD5 uniquifier ${CMAKE_CURRENT_BINARY_DIR}/${sourceFn})
    set(targetName "link_qa_${uniquifier}")

    add_custom_target(${targetName}
        DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${filename})
    add_dependencies(qaData ${targetName})
endmacro(link_qa_file filename)

macro(build_qa_retina retinadir)
    set(fullRetinaDir ${CMAKE_CURRENT_SOURCE_DIR}/${retinadir})
    set(retinaName ${retinadir}.tar.gz)
    set(fullRetinaOutput ${CMAKE_CURRENT_BINARY_DIR}/${retinaName})

    add_custom_command(
        OUTPUT ${fullRetinaOutput}
        COMMAND
            tar czf ${fullRetinaOutput} -C ${fullRetinaDir} . --xform s:'./'::
    )
    string(MD5 uniquifier ${fullRetinaOutput})
    set(targetName "link_qa_${uniquifier}")

    add_custom_target(${targetName}
        DEPENDS ${fullRetinaOutput})
    add_dependencies(qaData ${targetName})
endmacro(build_qa_retina retinadir)

macro(build_qa_workbook workbookdir)
    set(fullWorkbookDir ${CMAKE_CURRENT_SOURCE_DIR}/${workbookdir})
    set(workbookName ${workbookdir}.tar.gz)
    set(fullWorkbookOutput ${CMAKE_CURRENT_BINARY_DIR}/${workbookName})

    add_custom_command(
        OUTPUT ${fullWorkbookOutput}
        COMMAND
        tar czf ${fullWorkbookOutput} -C ${fullWorkbookDir} workbook/
    )
    string(MD5 uniquifier ${fullWorkbookOutput})
    set(targetName "link_qa_${uniquifier}")

    add_custom_target(${targetName}
        DEPENDS ${fullWorkbookOutput})
    add_dependencies(qaData ${targetName})
endmacro(build_qa_workbook)

macro(build_qa_df_workbook workbookdir)
    set(fullWorkbookDir ${CMAKE_CURRENT_SOURCE_DIR}/${workbookdir})
    set(workbookName ${workbookdir}.xlrdf.tar.gz)
    set(fullWorkbookOutput ${CMAKE_CURRENT_BINARY_DIR}/${workbookName})

    add_custom_command(
        OUTPUT ${fullWorkbookOutput}
        COMMAND
        tar czf ${fullWorkbookOutput} -C ${fullWorkbookDir} workbook/
    )
    string(MD5 uniquifier ${fullWorkbookOutput})
    set(targetName "link_qa_${uniquifier}")

    add_custom_target(${targetName}
        DEPENDS ${fullWorkbookOutput})
    add_dependencies(qaData ${targetName})
endmacro(build_qa_df_workbook)

macro(link_config_cfg filename)
    set(hostnameCfg "${CMAKE_SOURCE_DIR}/src/data/${HOSTNAME}.cfg")

    string(MD5 uniquifier ${filename})
    set(targetName "test_config_${uniquifier}")

    if(EXISTS ${hostnameCfg})
        set(sourceCfg ${hostnameCfg})
    else()
        set(sourceCfg ${CMAKE_SOURCE_DIR}/src/data/test.cfg)
    endif()

    # XXX this way of linking means that this file will never be out of date and
    # thus if the config file changes above, it will not be rebuilt
    add_custom_target(${targetName}
        DEPENDS ${filename}
    )
    add_custom_command(
        OUTPUT ${filename}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${sourceCfg}
            ${filename}
    )
    add_dependencies(xceBins ${targetName})
endmacro(link_config_cfg)

macro(link_license_file filename)
    set(sourceLic "${CMAKE_SOURCE_DIR}/src/data/XcalarLic.key")

    string(MD5 uniquifier ${filename})
    set(targetName "test_license_${uniquifier}")

    # XXX this way of linking means that this file will never be out of date and
    # thus if the license file changes above, it will not be rebuilt
    add_custom_target(${targetName}
        DEPENDS ${filename}
    )
    add_custom_command(
        OUTPUT ${filename}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${sourceLic}
            ${filename}
    )
    add_dependencies(xceBins ${targetName})
endmacro(link_license_file)
