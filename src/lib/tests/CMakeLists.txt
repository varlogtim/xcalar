find_package(CryptoPP REQUIRED)
add_definitions(-DWHOAMI=\"${WHOAMI}\")
add_definitions(-DXLR_QA)
include_directories("${CMAKE_SOURCE_DIR}/src/lib/tests/libqa")

set(TEST_LIBS ${COMMON_LIBS} stdc++ libqa libcommon ${CryptoPP_LIBRARIES})

add_subdirectory(libqa)
add_subdirectory(FuncTests)
add_subdirectory(libsimnode)
add_subdirectory(libconfig)
add_subdirectory(libjsongen)
add_subdirectory(libudf)
add_subdirectory(libchildnode)
add_subdirectory(libapis)
add_subdirectory(libmsg)

set(FNV_FILES
    "${CMAKE_SOURCE_DIR}/src/3rd/fnv/FnvTest.h"
    "${CMAKE_SOURCE_DIR}/src/3rd/fnv/FnvTest.cpp")

set(LIC_TEST_KEYS
    "${CMAKE_SOURCE_DIR}/src/data/EcdsaPub.key"
    "${CMAKE_SOURCE_DIR}/src/data/EcdsaPriv.key"
    "${CMAKE_SOURCE_DIR}/src/data/XcalarLic.key"
    "${CMAKE_SOURCE_DIR}/src/lib/tests/EcdsaPub.key.bad"
    "${CMAKE_SOURCE_DIR}/src/lib/tests/EcdsaPub.key.nomatch"
    "${CMAKE_SOURCE_DIR}/src/lib/tests/XcalarLicDemo.key"
    "${CMAKE_SOURCE_DIR}/src/lib/tests/XcalarLic.key.20node"
    "${CMAKE_SOURCE_DIR}/src/lib/tests/XcalarLic.key.2Bnode"
    "${CMAKE_SOURCE_DIR}/src/lib/tests/XcalarLic.key.bad"
    "${CMAKE_SOURCE_DIR}/src/lib/tests/XcalarLic.key.expired"
    "${CMAKE_SOURCE_DIR}/src/lib/tests/XcalarLic.key.product"
    "${CMAKE_SOURCE_DIR}/src/lib/tests/XcalarLicMod.key"
    "${CMAKE_SOURCE_DIR}/src/lib/tests/XcalarLicOper.key"
    "${CMAKE_SOURCE_DIR}/src/lib/tests/XcalarLic.key.sigKey"
    "${CMAKE_SOURCE_DIR}/src/lib/tests/XcalarLic.key.sigKeyBad"
)

set(LINKED_FILES ${FNV_FILES} ${LIC_TEST_KEYS})

set(FNV_LOCALS "")

set(TEST_KEY_LOCALS "")

set(TESTS
    libdssanity
    libhashSanity
    libstringsTest
    libsysTest
    libutilTest
    libxdbsanity
    liblogsanity
    libnewtuplebuftest
    libkvstoresanity
    libevaltests
    libcalloutsanity
    libruntimetests
    libbcsanity
    hashtabletests
    libdagsanity
    liblicenseKey
    libnstest
    libdatapagetest
    csvparsertests
    varinttests
)

foreach(fnvFile ${FNV_FILES})
    get_filename_component(localFile ${fnvFile} NAME)
    list(APPEND FNV_LOCALS "${CMAKE_CURRENT_BINARY_DIR}/${localFile}")
endforeach(fnvFile)

foreach(licTestKey ${LIC_TEST_KEYS})
    get_filename_component(localFile ${licTestKey} NAME)
    list(APPEND TEST_KEY_LOCALS "${CMAKE_CURRENT_BINARY_DIR}/${localFile}")
endforeach(licTestKey)

# Symlink in all files
foreach(linkedFile ${LINKED_FILES})
    get_filename_component(localFile ${linkedFile} NAME)
    add_custom_command(
        OUTPUT ${localFile}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${linkedFile} ${localFile}
        DEPENDS ${linkedFile})
endforeach(linkedFile)

add_custom_target(licenseLinkedFiles ALL
    DEPENDS ${TEST_KEY_LOCALS})
add_dependencies(qa licenseLinkedFiles)

link_qa_file(sanity.tst)
link_config_cfg(${CMAKE_CURRENT_BINARY_DIR}/test-config.cfg)

############################## Compiling tests #################################

add_xce_test_executable(libdssanity LibDsTest.cpp DataSetTestsCommon.cpp)
target_link_libraries(libdssanity ${TEST_LIBS})
target_include_directories(libdssanity
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/lib/libdf
    ${CMAKE_SOURCE_DIR}/src/lib/libds)

add_xce_test_executable(libdsrust LibDsTest.cpp DataSetTestsCommon.cpp)
target_link_libraries(libdsrust ${TEST_LIBS})
target_include_directories(libdsrust
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/lib/libdf
    ${CMAKE_SOURCE_DIR}/src/lib/libds)
target_compile_options(libdsrust PRIVATE "-DRUST")

add_xce_test_executable(libkvstoresanity LibKvStoreSanity.cpp KvStoreTestsCommon.cpp)
target_link_libraries(libkvstoresanity ${TEST_LIBS})

add_xce_test_executable(libnewtuplebuftest LibNewTupleBufTest.cpp)
target_link_libraries(libnewtuplebuftest ${TEST_LIBS})
target_include_directories(libnewtuplebuftest
    PRIVATE
    ${CMAKE_BINARY_DIR}/src/lib/libds
    ${CMAKE_SOURCE_DIR}/src/lib/libds)

add_xce_test_executable(libhashSanity LibHashTest.cpp ${FNV_LOCALS})
target_link_libraries(libhashSanity ${TEST_LIBS})
target_include_directories(libhashSanity
    PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR})

add_xce_test_executable(libstringsTest LibStringsTest.cpp)
target_link_libraries(libstringsTest ${TEST_LIBS})

add_xce_test_executable(libsysTest LibSysTest.cpp)
target_link_libraries(libsysTest ${TEST_LIBS})
target_include_directories(libsysTest
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/lib/libsys)

add_xce_test_executable(libutilTest LibUtilTest.cpp)
target_link_libraries(libutilTest ${TEST_LIBS})

add_xce_test_executable(libxdbsanity LibXdbSanity.cpp XdbTestsCommon.cpp)
target_link_libraries(libxdbsanity ${TEST_LIBS})
target_include_directories(libxdbsanity
    PRIVATE
    ${CMAKE_BINARY_DIR}/src/lib/libxdb
    ${CMAKE_SOURCE_DIR}/src/lib/libxdb)

add_xce_test_executable(libxdbrust LibXdbSanity.cpp XdbTestsCommon.cpp)
target_link_libraries(libxdbrust ${TEST_LIBS})
target_include_directories(libxdbrust
    PRIVATE
    ${CMAKE_BINARY_DIR}/src/lib/libxdb
    ${CMAKE_SOURCE_DIR}/src/lib/libxdb)
target_compile_options(libxdbrust PRIVATE "-DRUST")

add_xce_test_executable(liblogsanity LibLogSanity.cpp LogTestsCommon.cpp)
target_link_libraries(liblogsanity ${TEST_LIBS})

add_xce_test_executable(libcalloutsanity LibCalloutTest.cpp)
target_link_libraries(libcalloutsanity ${TEST_LIBS})

add_xce_test_executable(libevaltests EvalTests.cpp)
target_link_libraries(libevaltests ${TEST_LIBS})
target_include_directories(libevaltests
    PRIVATE
    ${CMAKE_BINARY_DIR}/src/lib/libdf
    ${CMAKE_SOURCE_DIR}/src/lib/libdf)

add_xce_test_executable(libbcsanity LibBcTest.cpp)
target_link_libraries(libbcsanity ${TEST_LIBS})

add_xce_test_executable(hashtabletests HashTableTests.cpp)
target_link_libraries(hashtabletests ${TEST_LIBS})

add_xce_test_executable(libruntimetests RuntimeTests.cpp)
set_source_files_properties("RuntimeTests.cpp" PROPERTIES COMPILE_FLAGS -Wno-format-security)
target_link_libraries(libruntimetests ${TEST_LIBS})
target_include_directories(libruntimetests
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/lib/libruntime)

add_xce_test_executable(libdagsanity LibDagTests.cpp)
target_link_libraries(libdagsanity ${TEST_LIBS})

add_xce_test_executable(liblicenseKey LibLicenseKey.cpp)
target_link_libraries(liblicenseKey ${TEST_LIBS})
target_include_directories(liblicenseKey
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/lib/liblicense)

add_xce_test_executable(libnstest LibNsTest.cpp)
target_link_libraries(libnstest ${TEST_LIBS})
target_include_directories(libnstest
    PRIVATE
    ${CMAKE_BINARY_DIR}/src/lib/libns
    ${CMAKE_SOURCE_DIR}/src/lib/libns)

add_xce_test_executable(libdatapagetest LibDataPageTest.cpp)
target_link_libraries(libdatapagetest stdc++ libqa libdatapage ${TEST_LIBS})

add_xce_test_executable(csvparsertests CsvParserTests.cpp)
target_link_libraries(csvparsertests stdc++ ${TEST_LIBS})

add_xce_test_executable(varinttests VarIntTests.cpp)
target_link_libraries(varinttests stdc++ ${TEST_LIBS})

########################## Installing test binaries ############################

foreach(test ${TESTS})
    # Make all tests a part of the sanity suite
    add_test(NAME ${test} COMMAND ${test})
endforeach(test)
