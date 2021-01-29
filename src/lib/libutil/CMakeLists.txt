find_package(CryptoPP CONFIG REQUIRED)
find_package(LibArchive)

if(NOT LibArchive_FOUND)
    find_package(LibArchive3 REQUIRED)
    set(LibArchive_LIBRARIES tinfo ${LibArchive_LIBRARIES})
endif()

include_directories("${CMAKE_SOURCE_DIR}/src/lib/libstat")
include_directories("${CMAKE_SOURCE_DIR}/src/lib/libutil")

add_xce_library(libutilxc STATIC
    System.cpp
    Lookaside.cpp
    WorkQueue.cpp
    Math.cpp
    Random.cpp
    CmdParser.cpp
    MemTrack.cpp
    License2.cpp
    LicenseGvm.cpp
    Archive.cpp
    Base64.cpp
    TrackHelpers.cpp
    FileUtils.cpp
    SchedulableFsm.cpp
    Uuid.cpp
    SystemPathVerify.cpp
    SystemVersionVerify.cpp
    CronMgr.cpp
    MemoryPile.cpp
    MemoryPool.cpp
    XcalarSocketInfo.cpp
    XcalarProcFsHelper.cpp
    XcalarSysHelper.cpp
    RemoteBitmap.cpp
    CgroupMgr.cpp
    AccumulatorHashTable.cpp
    DFPUtils.cpp
    SystemStatsApp.cpp
    SystemPerfApp.cpp
    GrpcServerApp.cpp
    SaveTrace.cpp
)

target_link_libraries(libutilxc
    m
    tinfo
    bz2
    z
    ssl
    crypto
    lzma
    xml2
    libdecnumber

    # Xcalar libs
    liblicense
    libruntime

    # 3rd-party libs
    ${CryptoPP_LIBRARIES}
    ${LibArchive_LIBRARIES}
    ${CUSTOM_HEAP_LIBRARIES}
)
