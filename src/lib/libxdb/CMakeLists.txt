add_xce_library(libxdb STATIC Xdb.cpp LibXdbGvm.cpp HashMergeSort.cpp
    HashTree.cpp HashTreeIndex.cpp HashTreeSelect.cpp HashTreeMgr.cpp LibHashTreeGvm.cpp
    XdbSerDes.cpp XdbSerDesSparse.cpp Merge.cpp MergeGvm.cpp MergeMgr.cpp)
set_source_files_properties("Xdb.cpp" PROPERTIES COMPILE_FLAGS -Wno-invalid-offsetof)
target_link_libraries(libxdb
    libruntime
    libnewtupbuf
    libmsg
    libgvm
    m)
