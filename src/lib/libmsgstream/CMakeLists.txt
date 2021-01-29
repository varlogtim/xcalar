include_directories("${CMAKE_SOURCE_DIR}/src/lib/libstat")
include_directories("${CMAKE_SOURCE_DIR}/src/lib/libutil")
include_directories("${CMAKE_SOURCE_DIR}/src/lib/libapp")
add_xce_library(libmsgstream STATIC
    MsgStream.cpp
    MsgStreamStartFsm.cpp
    MsgStreamEndFsm.cpp
    MsgStreamActionImpl.cpp
    MsgStreamState.cpp
    MsgStreamInfo.cpp
    MsgStreamSendFsm.cpp)
target_link_libraries(libmsgstream
    libutilxc)
