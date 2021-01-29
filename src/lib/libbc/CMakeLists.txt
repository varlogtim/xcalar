add_xce_library(libbc STATIC BufCacheObjectMgr.cpp BufCacheHandle.cpp BufCacheMemMgr.cpp BufferCache.cpp)
target_link_libraries(libbc
    libstat)
