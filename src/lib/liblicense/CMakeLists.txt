# these are the new files for the V2 license format
# LicenseKeyMetadata.h LicenseData2.h LicenseData2.cpp
# LicenseReader2.h LicenseReader2.cpp LicenseSignature.h LicenseSignature.cpp
# The following files from the V1 license also carry over:
# LicenseKeyFile.cpp LicenseKeyFile.h LicenseConstants.h
#
set(LINKED_FILES
    ${CMAKE_BINARY_DIR}/src/lib/libenum/LicenseTypes.cpp
)

set(licenseEmbedded "${CMAKE_CURRENT_BINARY_DIR}/LicenseEmbedded.cpp")

add_custom_command(
    OUTPUT ${licenseEmbedded}
    COMMAND ${GENEMBEDDED} -p `cat ${PASSWORDFILE}` -k "${PUBKEYFILE}" -c "${licenseEmbedded}"
    DEPENDS ${PASSWORDFILE} ${PUBKEYFILE} ${GENEMBEDDED}
    )

set(LOCAL_FILES "")
foreach(fileSource ${LINKED_FILES})
    get_filename_component(fileBase ${fileSource} NAME)
    set(localFile ${CMAKE_CURRENT_BINARY_DIR}/${fileBase})
    add_custom_command(
        OUTPUT ${localFile}
        COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${fileSource} ${localFile}
        DEPENDS ${fileSource})
    list(APPEND LOCAL_FILES ${localFile})
endforeach(fileSource)

add_xce_library(liblicense STATIC ${LOCAL_FILES} LicenseKeyFile.cpp LicenseSignature.cpp LicenseData2.cpp LicenseReader2.cpp ${licenseEmbedded})

add_xce_library(liblicensereader STATIC ${LOCAL_FILES} LicenseSignature.cpp LicenseData2.cpp LicenseReader2.cpp LicenseKeyFile.cpp ${licenseEmbedded})
