add_subdirectory(xdf)
include_directories("${CMAKE_SOURCE_DIR}/src/include/parent")
add_xce_library(liboperators STATIC LocalOperators.cpp OperatorsHash.cpp DhtHash.cpp DhtHashGvm.cpp GlobalOperators.cpp XcalarEval.cpp BuiltInFunctions.cpp OperatorsXdbPageOps.cpp OperatorsGvm.cpp OperatorsEvalTypes.cpp)
set_source_files_properties("BuiltInFunctions.cpp" PROPERTIES COMPILE_FLAGS -Wno-missing-field-initializers)
target_link_libraries(liboperators
    libxdfs
    libdag
    libscalars
    libdf
    libds
    libcursor
    libxdb
    libbc
    libparent
    libchild
    libudf
    libutilxc
    libexport
    libchildpool)
