#!/bin/bash

udfApacheLogParserTest()
{
    local datasetFolder="$pathToQaDatasets/apacheLogs"
    local datasetPath="$datasetFolder/access.log"
    local udfPath="$datasetFolder/parseAccessLog.py"
    local datasetUrl="nfs://$datasetPath"
    local moduleName="parseAccessLog"
    local fnName="parseAccessLog"

    uploadPython "$moduleName" "$udfPath"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not upload \"$udfPath\""
        return $ret
    fi

    listPython "$moduleName:$fnName"
    echo "$output"
    numUdfs=`echo "$output" | grep "Num UDFs" | cut -d: -f2 | tr -d ' '`
    if [ "$numUdfs" != "1" ]; then
        printInColor "red" "Could not find uploaded python function ($moduleName:$fnName)"
        return 1
    fi

    downloadPython "$moduleName" "$TMP1/foo.py"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not download \"$moduleName\""
        return 1
    fi

    local pythonFileSize=`cat $udfPath | wc -c`
    cmp -n $pythonFileSize "$udfPath" "$TMP1/foo.py"
    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Downloaded file differs from $udfPath"
        return 1
    fi

    rm "$TMP1/foo.py"

    load "json" "$datasetUrl" "" "" "" "" "$moduleName:$fnName"
    local ret=$?
    local datasetName="$output"

    if [ "$ret" != "0" -o "$datasetName" = "" ]; then
        printInColor "red" "Error loading \"$datasetUrl\""
        return 1
    fi

    inspectDataset "$datasetName"
    numRows=`echo "$output" | grep "Total" | cut -d: -f2 | tr -d ' '`
    expectedNumRows=`cat $datasetPath | wc -l`

    if [ "$numRows" != "$expectedNumRows" ]; then
        printInColor "red" "Number of rows in $datasetName ($numRows) != number of rows in $datasetPath ($expectedNumRows)"
        return 1
    fi

    # Xc-7569. We need to cat the dataset to force the protoToJson codepath
    catResult "$datasetName" 0 1
    local jsonRecord=`getJsonRecord "$output"`
    local requestUrlUsername=`getJsonField "$jsonRecord" "request_url_username"`

    echo "$jsonRecord"
    if [ "$requestUrlUsername" != "null" ]; then
        printInColor "red" "request_url_username = \"$requestUrlUsername\". Expected null"
        return 1
    fi

    echo "PASS" 1>&2
    return 0
}

