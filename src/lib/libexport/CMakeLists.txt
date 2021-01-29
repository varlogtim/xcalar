add_xce_library(libexport STATIC
    Export.cpp
    DataTarget.cpp
    SourceFormatConnector.cpp
    UDFConnector.cpp
    DataTargetGvm.cpp
    DataTargetDurable.cpp
    DataTargetDurableMinorVers.cpp)
target_link_libraries(libexport
    libenum
    libutilxc)
