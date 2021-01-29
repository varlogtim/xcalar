add_xce_library(libsupport STATIC SupportBundle.cpp LibSupportGvm.cpp)
target_link_libraries(libsupport
    libconstants
    )
