add_subdirectory(python)
find_package(PythonLib REQUIRED)
include_directories("${PYTHON_INCLUDE_DIRS}" "${CMAKE_SOURCE_DIR}/src/include/parent")
add_xce_library(libudf STATIC UserDefinedFunction.cpp UdfLocal.cpp UdfOperation.cpp UdfPersist.cpp UdfParent.cpp)
target_link_libraries(libudf
    libudfpy
    )
