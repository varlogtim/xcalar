#add_subdirectory(jsoniter)
#add_subdirectory(jsongenondisk)
#add_subdirectory(retinaCheck)
#add_subdirectory(absurdMalloc)
#add_subdirectory(absurdMlock)
add_subdirectory(xcMapRClient)
add_subdirectory(logdump)
add_subdirectory(dataflowUpgradeTool)
add_subdirectory(xcUpgradeTool)
add_subdirectory(parquetReader)
# XXX FIXME prod build is breaking in el-7
# but working fine in ubuntu dev machine
# add_subdirectory(memoryBenchmark)
add_subdirectory(ioBenchmark)
