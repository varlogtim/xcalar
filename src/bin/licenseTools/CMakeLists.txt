find_package(CryptoPP CONFIG REQUIRED)

add_xce_executable(licenseCheck licenseCheck.cpp)
add_xce_executable(readKey ReadKey.cpp)
add_xce_executable(signKey SignKey.cpp)
add_xce_executable(cryptKeyGen CryptKeyGen.cpp)

target_link_libraries(licenseCheck
   libenum
   liblicensereader
   libstringutilxc
   -static-libgcc
   -static-libstdc++
   ${CryptoPP_LIBRARIES}
)

target_link_libraries(readKey
   libenum
   liblicensereader
   libstringutilxc
   -static-libgcc
   -static-libstdc++
   ${CryptoPP_LIBRARIES}
)

target_link_libraries(signKey
   libenum
   liblicensereader
   libstringutilxc
   -static-libgcc
   -static-libstdc++
   ${CryptoPP_LIBRARIES}
)

target_link_libraries(cryptKeyGen
   libenum
   liblicensereader
   libstringutilxc
   -static-libgcc
   -static-libstdc++
   ${CryptoPP_LIBRARIES}
)

install(TARGETS licenseCheck DESTINATION "bin")
