add_custom_command(
    OUTPUT childnode
    COMMAND ln -f
        ${CMAKE_BINARY_DIR}/src/bin/usrnode/usrnode childnode
    DEPENDS usrnode)
add_custom_target(childnode_bin
    DEPENDS childnode)
add_dependencies(xceBins childnode_bin)

# Since we're treating childnode like a file and not an executable, the permissions
# need to be manually set. Ideally we would find a way to have add_xce_executable
# without having sources. Or we can rebuild childnode from source.
install(
    FILES ${CMAKE_CURRENT_BINARY_DIR}/childnode
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
    DESTINATION "bin")
