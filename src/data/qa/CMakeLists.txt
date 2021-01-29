add_subdirectory(tpchDatasets)
add_subdirectory(retinaTests)
add_subdirectory(workbookUploadTests)
add_subdirectory(tpch)
add_subdirectory(dfWorkbookTests)
add_subdirectory(dataflowExecuteTests)
add_subdirectory(smallFiles)

set(PLACEHOLDER_DATA
    ${CMAKE_CURRENT_SOURCE_DIR}/parquet.tar.gz.sha1
)

set(DATA_ARCHIVES
    ${CMAKE_CURRENT_SOURCE_DIR}/actors.tar.gz
    ${CMAKE_CURRENT_SOURCE_DIR}/flight.tar.gz
    ${CMAKE_CURRENT_SOURCE_DIR}/edgeCases.tar.gz
    ${CMAKE_CURRENT_SOURCE_DIR}/yelp.tar.gz
    ${CMAKE_CURRENT_SOURCE_DIR}/dataGov.tar.gz
    ${CMAKE_CURRENT_SOURCE_DIR}/gdelt-small.tar.gz
    ${CMAKE_CURRENT_SOURCE_DIR}/exportTests.tar.gz
    ${CMAKE_CURRENT_SOURCE_DIR}/caseStudy.tar.gz
    ${CMAKE_CURRENT_SOURCE_DIR}/customer1.tar.gz
    ${CMAKE_CURRENT_BINARY_DIR}/parquet.tar.gz
)

link_qa_file(customer2)
link_qa_file(test-multisort.csv)

build_qa_retina(LgDfg)
build_qa_retina(EarningsToCapFull)
build_qa_retina(errorRetina2)
build_qa_retina(nonstopRetina)
build_qa_retina(MatrixGenMultiply)
build_qa_retina(retinaWithDefUdf)
build_qa_retina(customer13)

link_qa_file(badLicenses)
link_qa_file(sessionReplay)
link_qa_file(keyCollision)
link_qa_file(columns)
link_qa_file(qaJsonGenSchema)
link_qa_file(utilTest)
link_qa_file(jsonSanity)
link_qa_file(indexJoin)
link_qa_file(kMeans)
link_qa_file(liblog)
link_qa_file(operatorsTest)
link_qa_file(csvSanity)
link_qa_file(xmlSanity)
link_qa_file(testSigning)
link_qa_file(apps)
link_qa_file(apacheLogs)
link_qa_file(udfOnLoadDatasets)
link_qa_file(jsonRandom)
link_qa_file(symlink)
link_qa_file(recursiveSymlink)
link_qa_file(jupyterNotebooks)
link_qa_file(sisterDirs)
link_qa_file(sdkIMDTest)

foreach(placeholder ${PLACEHOLDER_DATA})
    string(REGEX REPLACE "\\.[^.]*$" "" actualFilePath ${placeholder})
    get_filename_component(actualFileName ${actualFilePath} NAME)
    add_custom_command(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${actualFileName}
        COMMAND ${FETCHFILE} ${placeholder} > ${CMAKE_CURRENT_BINARY_DIR}/${actualFileName}
        DEPENDS ${placeholder})
endforeach(placeholder)

set(DECOMPRESSED_DATA "")
foreach(archive ${DATA_ARCHIVES})
    get_filename_component(dataName ${archive} NAME_WE)
    # XXX We really want this to be out-of-source. Revisit this when we redesign
    # QA dataset management
    add_custom_command(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${dataName}
        COMMAND tar zixf ${archive}
        COMMAND touch ${dataName}
        DEPENDS ${archive})
    list(APPEND DECOMPRESSED_DATA ${CMAKE_CURRENT_BINARY_DIR}/${dataName})
endforeach(archive)

add_custom_target(decompressedData
    DEPENDS ${DECOMPRESSED_DATA})

add_dependencies(qaData decompressedData)
