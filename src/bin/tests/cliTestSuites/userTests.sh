# A user's table is private. Another user should not be able to know of its
# existence
privateTablesTest()
{
    local user1="cliUser1"
    local user2="cliUser2"
    local datasetUrl="nfs://$pathToQaDatasets/jsonSanity/jsonSanity.json"
    local datasetFormat="json"
    local key="int"
    local tableName="userTests/tempTable"
    local sessionNamePrefix="userTests-"

    echo "Creating session ${sessionNamePrefix}${user1}"
    sessionNewOrReplace "$user1" "${sessionNamePrefix}${user1}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to create session \"${sessionNamePrefix}${user1}\""
        return $ret
    fi

    echo "Activating session ${sessionNamePrefix}${user1}"
    sessionActivate "$user1" "${sessionNamePrefix}${user1}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to create session \"${sessionNamePrefix}${user1}\""
        return $ret
    fi

    echo "Creating session ${sessionNamePrefix}${user2}"
    sessionNewOrReplace "$user2" "${sessionNamePrefix}${user2}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to create session \"${sessionNamePrefix}${user2}\""
        return $ret
    fi

    loadAs "$user1" "$datasetFormat" "$datasetUrl"
    local ret=$?
    local dataset="$output"
    if [ "$ret" != "0" -o "$dataset" = "" ]; then
        printInColor "red" "Failed to load \"$datasetUrl\""
        return 1
    fi
    echo "Created dataset \"$dataset\""

    echo "Activating session ${sessionNamePrefix}${user2}"
    sessionActivate "$user2" "${sessionNamePrefix}${user2}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to activate session \"${sessionNamePrefix}${user2}\""
        return $ret
    fi

    dropLoadNodeAs "$user2" "$dataset"
    local ret=$?
    if [ "$ret" = "0" ]; then
        # Should not be able to drop because the load node
        # doesn't exist in this user's sessionGraph yet
        printInColor "red" "Should not be able to drop \"$dataset\""
        ret=1
        return $ret
    fi

    # Should succeed because datasets are shared
    indexAs "$user2" "$dataset" "$key" "$tableName"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to index \"$tableName\""
        return $ret
    fi

    # Should not succeed because user1 should not be able to see user2's table
    listTablesAs "$user1"
    userTables="$output"
    echo "$userTables" | grep -q "$tableName"
    if [ "$?" = "0" ]; then
        # We're not supposed to find it!
        printInColor "red" "$user1 is able to see tables created by $user2"
        return 1
    fi

    # Should not succeed because user1 should not be able to drop user2's table
    dropAs "$user1" "$tableName"
    local ret=$?
    if [ "$ret" = "0" ]; then
        printInColor "red" "$user1 was able to drop $user2's table"
        return 1
    fi

    dropAs "$user2" "$tableName"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not drop \"$tableName\""
        return $ret
    fi

    dropLoadNodeAs "$user2" "$dataset"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not drop load node \"$dataset\""
        ret=1
        return $ret
    fi

    dropLoadNodeAs "$user1" "$dataset"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to drop \"$dataset\""
        return $ret
    fi

    # Nuke both sessions
    sessionInact "$user1" "${sessionNamePrefix}${user1}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not terminate session \"${sessionNamePrefix}${user1}\""
        return $ret
    fi

    sessionDelete "$user1" "${sessionNamePrefix}${user1}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not delete session \"${sessionNamePrefix}${user1}\""
        return $ret
    fi

    sessionInact "$user2" "${sessionNamePrefix}${user2}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not terminate session \"${sessionNamePrefix}${user2}\""
        return $ret
    fi

    sessionDelete "$user2" "${sessionNamePrefix}${user2}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not delete session \"${sessionNamePrefix}${user2}\""
        return $ret
    fi

    delistDs "$dataset"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not delist dataaset \"$dataset\""
        return $ret
    fi

    echo "PASS" 1>&2
    return 0
}


privateTablesTest2()
{
    local user1="cliUser1"
    local user2="cliUser2"
    local datasetUrl="nfs://$pathToQaDatasets/jsonSanity/jsonSanity.json"
    local datasetFormat="json"
    local key1="int"
    local tableName1="userTests/tempTable1"
    local key2="float"
    local tableName2="userTests/tempTable2"
    local sessionNamePrefix="userTests-"

    echo "Creating session ${sessionNamePrefix}${user1}"
    sessionNewOrReplace "$user1" "${sessionNamePrefix}${user1}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to create session \"${sessionNamePrefix}${user1}\""
        return $ret
    fi

    echo "Activating session ${sessionNamePrefix}${user1}"
    sessionActivate "$user1" "${sessionNamePrefix}${user1}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to activate session \"${sessionNamePrefix}${user1}\""
        return $ret
    fi

    echo "Creating session ${sessionNamePrefix}${user2}"
    sessionNewOrReplace "$user2" "${sessionNamePrefix}${user2}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to create session \"${sessionNamePrefix}${user2}\""
        return $ret
    fi

    echo "Activating session ${sessionNamePrefix}${user2}"
    sessionActivate "$user2" "${sessionNamePrefix}${user2}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to activate session \"${sessionNamePrefix}${user2}\""
        return $ret
    fi

    loadAs "$user1" "$datasetFormat" "$datasetUrl"
    local ret=$?
    local dataset="$output"
    if [ "$ret" != "0" -o "$dataset" = "" ]; then
        printInColor "red" "Failed to load \"$datasetUrl\""
        return 1
    fi

    # Should succeed because datasets are shared
    indexAs "$user2" "$dataset" "$key1" "$tableName1"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to index \"$tableName1\""
        return $ret
    fi

    # Should not succeed because user1 should not be able to see user2's table
    listTablesAs "$user1"
    userTables="$output"
    echo "$userTables" | grep -q "$tableName1"
    if [ "$?" = "0" ]; then
        # We're not supposed to find it!
        printInColor "red" "$user1 is able to see tables created by $user2"
        return 1
    fi

    # Should not succeed because user1 should not be able to drop user2's table
    dropAs "$user1" "$tableName1"
    local ret=$?
    if [ "$ret" = "0" ]; then
        printInColor "red" "$user1 was able to drop $user2's table"
        return 1
    fi

    dropLoadNodeAs "$user1" "$dataset"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "could not drop \"$dataset\""
        ret=1
        return $ret
    fi


    indexAs "$user2" "$dataset" "$key2" "$tableName2"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to index \"$tableName2\""
        return $ret
    fi

    dropAs "$user2" "*"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not drop table"
        return $ret
    fi

    dropLoadNodeAs "$user2" "$dataset"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to drop \"$dataset\""
        return $ret
    fi

    # Nuke both sessions
    sessionInact "$user1" "${sessionNamePrefix}${user1}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not terminate session \"${sessionNamePrefix}${user1}\""
        return $ret
    fi

    sessionDelete "$user1" "${sessionNamePrefix}${user1}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not delete session \"${sessionNamePrefix}${user1}\""
        return $ret
    fi

    sessionInact "$user2" "${sessionNamePrefix}${user2}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not terminate session \"${sessionNamePrefix}${user2}\""
        return $ret
    fi

    sessionDelete "$user2" "${sessionNamePrefix}${user2}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not delete session \"${sessionNamePrefix}${user2}\""
        return $ret
    fi

    delistDs "$dataset"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not delist dataset \"$dataset\""
        return $ret
    fi

    echo "PASS" 1>&2
    return 0
}

# A user should be able to look at datasets loaded by other users
# Witness to Xc-4986
datasetSharedTest()
{
    local user1="cliUser1"
    local user2="cliUser2"
    local datasetUrl="nfs://$pathToQaDatasets/jsonSanity/jsonSanity.json"
    local datasetFormat="json"

    echo "Creating session ${sessionNamePrefix}${user1}"
    sessionNewOrReplace "$user1" "${sessionNamePrefix}${user1}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to create session \"${sessionNamePrefix}${user1}\""
        return $ret
    fi

    echo "Activating session ${sessionNamePrefix}${user1}"
    sessionActivate "$user1" "${sessionNamePrefix}${user1}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to activate session \"${sessionNamePrefix}${user1}\""
        return $ret
    fi


    echo "Creating session ${sessionNamePrefix}${user2}"
    sessionNewOrReplace "$user2" "${sessionNamePrefix}${user2}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to create session \"${sessionNamePrefix}${user2}\""
        return $ret
    fi

    echo "Activating session ${sessionNamePrefix}${user2}"
    sessionActivate "$user2" "${sessionNamePrefix}${user2}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Failed to activate session \"${sessionNamePrefix}${user2}\""
        return $ret
    fi

    loadAs "$user1" "$datasetFormat" "$datasetUrl"
    local ret=$?
    local dataset="$output"
    if [ "$ret" != "0" -o "$dataset" = "" ]; then
        printInColor "red" "Failed to load \"$datasetUrl\""
        return 1
    fi

    # Nuke both sessions
    sessionInact "$user1" "${sessionNamePrefix}${user1}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not terminate session \"${sessionNamePrefix}${user1}\""
        return $ret
    fi

    sessionDelete "$user1" "${sessionNamePrefix}${user1}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not delete session \"${sessionNamePrefix}${user1}\""
        return $ret
    fi

    sessionInact "$user2" "${sessionNamePrefix}${user2}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not terminate session \"${sessionNamePrefix}${user2}\""
        return $ret
    fi

    sessionDelete "$user2" "${sessionNamePrefix}${user2}"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not delete session \"${sessionNamePrefix}${user2}\""
        return $ret
    fi

    delistDs "$dataset"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not delist dataaset \"$dataset\""
        return $ret
    fi

    echo "PASS" 1>&2
    return 0
}
