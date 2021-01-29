add_xce_library(libdatapage STATIC
    DataPage.cpp
    DataPageIndex.cpp
    MemPool.cpp
    DataValue.cpp)

target_link_libraries(libdatapage
    libutilxc)
