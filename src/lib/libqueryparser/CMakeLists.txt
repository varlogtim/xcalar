add_xce_library(libqueryparser STATIC QueryParser.cpp QpLoad.cpp QpIndex.cpp QpFilter.cpp QpGroupBy.cpp QpJoin.cpp QpUnion.cpp QpMap.cpp QpAggregate.cpp QpRename.cpp QpExport.cpp QpDrop.cpp QpProject.cpp QpGetRowNum.cpp QpDelist.cpp QpExecuteRetina.cpp QpSynthesize.cpp QpSelect.cpp)
target_link_libraries(libqueryparser
    libgetopt
    libutilxc
)
