loadLoadAggrMaxMin()
{
    local datasetUrl="nfs://$pathToQaDatasets/jsonRandom"
    local keyName="JgSignedInteger"
    local tableName="aggrMaxMinTests/signedIntTable"

    load "json" "$datasetUrl" "10MB"
    local dataset="$output"

    local ret=$?
    if [ $ret -ne 0 ] || [ "$dataset" = "" ]; then
        return 1
    fi

    index $dataset "${keyName}" "${tableName}"

    validateTableExists "${tableName}"
    ret=$?
    if [ $ret -ne 0 ]; then
        return 1
    else
        return 0
    fi
}

loadInspectAggrMaxMin()
{
    local keyName="JgSignedInteger"
    local tableName="aggrMaxMinTests/signedIntTable"
    local sortedTableName="aggrMaxMinTests/sortedSignedIntTable"

    xaggregate "${tableName}" "maxInteger(${keyName})" "${tableName}-maxInteger-table"
    ret=$?
    if [ $ret -ne 0 ]; then
        return 1
    fi
    local maxAggrOutput="$output"

    xaggregate "${tableName}" "minInteger(${keyName})" "${tableName}-minInteger-table"
    ret=$?
    if [ $ret -ne 0 ]; then
        return 1
    fi
    local minAggrOutput="$output"

    xaggregate "${tableName}" "count(${keyName})" "${tableName}-count-table"
    ret=$?
    if [ $ret -ne 0 ]; then
        return 1
    fi
    local totalRecord="$output"

    # Let's sort the table now to get the minAggrOutput
    indexTable "$tableName" "$keyName" "$sortedTableName" "true"

    local recordNum=0

    catResult "${sortedTableName}" $recordNum
    echo "$output" | head -n 3 | grep -- "$minAggrOutput"
    ret=$?

    if [ $ret -ne 0 ]; then
        printInColor "red" "Could not find $minAggrOutput in \"$output\""
        return 1

    fi

    catResult "${sortedTableName}" $(( $totalRecord - 1 ))
    echo "$output" | tail -n 1 | grep -- "$maxAggrOutput"
    ret=$?

    if [ $ret -ne 0 ]; then
        printInColor "red" "Could not find $maxAggrOutput in \"$output\""
        return 1
    fi

    echo "min='$minAggrOutput' max='$maxAggrOutput' total='$totalRecord'"

    set +x
    if [ $ret -ne 0 ]; then
        return 1
    else
        return 0
    fi
}
