set(TPCH_DATASETS
    ${CMAKE_CURRENT_BINARY_DIR}/customer.tbl
    ${CMAKE_CURRENT_BINARY_DIR}/nation.tbl
    ${CMAKE_CURRENT_BINARY_DIR}/lineitem.tbl
    ${CMAKE_CURRENT_BINARY_DIR}/orders.tbl
    ${CMAKE_CURRENT_BINARY_DIR}/partsupp.tbl
    ${CMAKE_CURRENT_BINARY_DIR}/part.tbl
    ${CMAKE_CURRENT_BINARY_DIR}/region.tbl
    ${CMAKE_CURRENT_BINARY_DIR}/supplier.tbl
)

# DBGEN_CMD must be kept in sync with TPCH_DATASETS
set (DBGEN_CMD
    c n L O S P r s)

set(srcDist "${CMAKE_SOURCE_DIR}/src/3rd/dbgen/dists.dss")
set(dstDist "${CMAKE_CURRENT_BINARY_DIR}/dists.dss")
set(srcAnswer "${CMAKE_SOURCE_DIR}/src/3rd/dbgen/answers")
set(dstAnswer "${CMAKE_CURRENT_BINARY_DIR}/answers")

add_custom_command(
    OUTPUT ${dstDist}
    COMMAND ${CMAKE_COMMAND} -E create_symlink ${srcDist} ${dstDist}
)

add_custom_command(
    OUTPUT ${dstAnswer}
    COMMAND ${CMAKE_COMMAND} -E create_symlink ${srcAnswer} ${dstAnswer}
)

set(ii 0)
foreach (dataset ${TPCH_DATASETS})
    list(GET DBGEN_CMD ${ii} dbGenLetter)
    add_custom_command(
        OUTPUT ${dataset}
        COMMAND ${CMAKE_BINARY_DIR}/src/3rd/dbgen/dbgen -q -f -T ${dbGenLetter}
        COMMAND chmod 664 ${dataset}
        DEPENDS ${dstDist} dbgen
    )
    math(EXPR ii "${ii} + 1")
endforeach(dataset)

add_custom_target(tpchDatasets ALL DEPENDS ${TPCH_DATASETS} ${dstAnswer})
add_dependencies(qaData tpchDatasets)
