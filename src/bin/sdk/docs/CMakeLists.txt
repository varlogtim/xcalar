add_custom_target(docs
    COMMAND
    sphinx-build -M html "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}" -q -jauto -n -a -E
    DEPENDS
        xc2Install # Let's make sure we install all our python pkgs
    COMMENT
        "Building SDK documentation")

# XXX this isn't necessary and is really just for fun
add_custom_target(viewDocs
    COMMAND
        google-chrome ${CMAKE_CURRENT_BINARY_DIR}/html/index.html
    DEPENDS
        docs)
