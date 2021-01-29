add_xce_library(libconfig STATIC Config.cpp ConfigFile.cpp)
target_link_libraries(libconfig
    jansson
    # Xcalar libs
    libfunctests
    libcommon)
