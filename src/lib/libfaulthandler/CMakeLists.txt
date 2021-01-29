
# Hard-code the compile options for this
set(BUILD_FLAGS "-std=gnu99 -fPIC -DPIC -g")
set(CMAKE_C_FLAGS_DEBUG ${BUILD_FLAGS})
set(CMAKE_C_FLAGS_RELEASE ${BUILD_FLAGS})
set(CMAKE_C_FLAGS_RELWITHDEBINFO ${BUILD_FLAGS})
add_xce_library(faulthandler SHARED FaultHandler.c)

add_dependencies(installedObjects faulthandler)

set_target_properties(faulthandler PROPERTIES VERSION "0.0.0"
                                              SOVERSION "0")

install(TARGETS faulthandler
    LIBRARY DESTINATION lib)
