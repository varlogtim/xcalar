#!/bin/bash

indexNonexistentField()
{
    # depends on basicChecks() table
    indexTable "yelp/user-votes.funny" "nonexistentKey" "yelp/user-nonexistentKey"
    ret=$?
    if [ $ret -ne 0 ]; then
        return 1
    else
        return 0
    fi

    echo "PASS" 1>&2
    return 0
}

indexAlreadyIndexedKey()
{
    # depends on basicChecks() table

    indexTable "yelp/user-votes.funny" "votes.funny" "yelp/user-votes.funny2"
    ret=$?
    if [ $ret -ne 0 ]; then
        return 0
    else
        return 1
    fi
}

indexVaryingTypes()
{
    local datasetUrl="nfs://$pathToQaDatasets/jsonSanity/jsonSanity.json"
    local datasetType="json"
    local typesSupported=("int" "float" "string" "arrayInt" "arrayFloat" "object.int" "object.float" "object.string" "bool")
    local baseTableName="indexVaryingTypes"

    loadDataset "$datasetUrl" "$datasetType" "$baseTableName" ${typesSupported[@]}
    local ret=$?
    if [ "$ret" -ne 0 ]; then
        return 1
    fi

    # Index table can happen in parallel
    for ii in `seq 1 $(( ${#typesSupported[@]} - 1))`; do
        indexTable "$baseTableName-${typesSupported[0]}" "p::${typesSupported[$ii]}" "$baseTableName-${typesSupported[0]}-${typesSupported[$ii]}"
    done

    for ii in `seq 1 $(( ${#typesSupported[@]} - 1))`; do
        validateTableExists "$baseTableName-${typesSupported[0]}-${typesSupported[$ii]}"
        if [ $ret -ne 0 ]; then
            echo "FAIL" 1>&2
            return 1
        fi
    done

    echo "PASS"
    return 0
}

indexNonAsciiTest()
{
    local url="nfs://$pathToQaDatasets/jsonSanity/nonAscii.json"
    load "json" $url
    local ret=$?
    local dsName="$output"
    if [ $ret -ne 0 ] || [ "$dsName" = "" ]; then
        printInColor "red" "Could not load $url: $ret"
        return 1
    fi

    index "$dsName" "短语" "phrase-$dsName"
    ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Index failed: $ret"
        return 1
    fi

    getRowCount "phrase-$dsName"
    local numRows="$output"
    echo "phrase-$dsName has $numRows rows"
    if [ "$numRows" != "4" ]; then
        printInColor "red" "phrase-$dsName has different number of rows($numRows) than expected(1)!"
        return 1
    fi

    catResult "phrase-$dsName" 0 1
    echo "output" | grep -q "你好世界"
    ret=$?
    if [ "$?" != "0" ]
    then
        printInColor "red" "phrase-$dsName missing 你好世界 ($output)!"
        return 1
    fi

    echo "PASS" 1>&2
    return 0
}
