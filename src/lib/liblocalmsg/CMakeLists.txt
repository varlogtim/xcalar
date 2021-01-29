add_xce_library(liblocalmsg STATIC LocalMsg.cpp LocalConnection.cpp)
target_link_libraries(liblocalmsg
    libservice
    libfunctests)
