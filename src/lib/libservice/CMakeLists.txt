include(genProto)

xc_protobuf_generate(
    CPP_SERVICE_HDRS
        serviceHs
    CPP_SERVICE_SRCS
        serviceCpps
    PROTO_PATH
        "${PROTO_DST_ROOT}"
    FILES
        ${PROTO_SERVICE_FILES}
    )

add_custom_target(GENERATED_HEADERS_XCRPC DEPENDS ${serviceHs})

add_xce_library(libservice STATIC
    RpcServiceStat.cpp
    ServiceMgr.cpp
    EchoService.cpp
    KvStoreService.cpp
    LogService.cpp
    MemoryService.cpp
    PublishedTableService.cpp
    UserDefinedFunctionService.cpp
    CgroupService.cpp
    QueryService.cpp
    TableService.cpp
    OperatorService.cpp
    LicenseService.cpp
    StatsService.cpp
    ResultSetService.cpp
    DataflowService.cpp
    DagNodeService.cpp
    ConnectorsService.cpp
    SchemaLoadService.cpp
    AppService.cpp
    VersionService.cpp
    ${serviceCpps}
)
target_link_libraries(libservice
    libxprotobuf
    libutilxc)
