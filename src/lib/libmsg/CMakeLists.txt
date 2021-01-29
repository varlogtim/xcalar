add_subdirectory(stubs)
add_xce_library(libmsg STATIC Message.cpp Sched.cpp SchedMessageClient.cpp TwoPcFuncDefs.cpp TwoPcFuncDefsClients.cpp Xid.cpp)
target_link_libraries(libmsg
    libmsgstubs
    libmsgstream
    libsys
    libdemystify)
