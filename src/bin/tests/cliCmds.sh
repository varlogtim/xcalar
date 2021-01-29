#!/bin/bash

# List of status code
statusErrorTimeout=10

# Attempts to create a session. If session exists, we delete
# the old one and replace with ourselves
sessionNewOrReplace()
{
    local userid=$1
    local sessionName=$2

    for ii in `seq 1 2`; do
        sessionNew "$userid" "$sessionName"
        local ret=$?
        if [ "$ret" = "0" -o "$ii" != "1" ]; then
            return $ret
        else
            echo "$output" | grep -q "already exists"
            local alreadyExists=$?
            if [ "$alreadyExists" = "0" ]; then
                echo "Trying to delete existing session and re-create"
                sessionDelete "$userid" "$sessionName"
                local ret2=$?
                if [ "$ret2" != "0" ]; then
                    printInColor "red" "Failed to delete \"$sessionName\""
                    return $ret2
                fi
            fi
        fi
    done
}

sessionNew()
{
    local userid=$1
    local sessionName=$2

    local cmdName="session"
    cmdsTested[$cmdName]=1

    if [ "$userid" = "" ]; then
        local userSuffix=""
    else
        local userSuffix="-u $userid"
    fi

    local sessionNewCmd="$cmdName --new --name \"$sessionName\""
    echo "$sessionNewCmd;" >> "${cliCmdsOutputFile}${userid}"
    output=`$CLI $userSuffix -c "$sessionNewCmd"`
    echo "$output"
    echo "$output" | grep -q "New session created"
    local ret=$?
    return $ret
}

sessionActivate()
{
    local userid=$1
    local sessionName=$2

    local cmdName="session"
    cmdsTested[$cmdName]=1

    if [ "$userid" = "" ]; then
        local userSuffix=""
    else
        local userSuffix="-u $userid"
    fi

    local sessionSwitchCmd="$cmdName --switch --name \"$sessionName\""
    echo "$sessionSwitchCmd;" >> "${cliCmdsOutputFile}${userid}"
    output=`$CLI $userSuffix -c "$sessionSwitchCmd"`
    echo "$output"
    echo "$output" | grep -q "Session successfully switched"
    local ret=$?
    return $ret
}

sessionInact()
{
    local userid=$1
    local sessionName=$2

    local cmdName="session"
    cmdsTested[$cmdName]=1

    if [ "$userid" = "" ]; then
        local userSuffix=""
    else
        local userSuffix="-u $userid"
    fi

    local sessionInactCmd="$cmdName --inact --name \"$sessionName\""
    echo "$sessionInactCmd;" >> "${cliCmdsOutputFile}${userid}"
    output=`$CLI $userSuffix -c "$sessionInactCmd"`
    echo "$output"
    echo "$output" | grep -q "inactivated"
    local ret=$?
    return $ret
}

sessionDelete()
{
    local userid=$1
    local sessionName=$2

    local cmdName="session"
    cmdsTested[$cmdName]=1

    if [ "$userid" = "" ]; then
        local userSuffix=""
    else
        local userSuffix="-u $userid"
    fi

    local sessionDeleteCmd="$cmdName --delete --name \"$sessionName\""
    echo "$sessionDeleteCmd;" >> "${cliCmdsOutputFile}${userid}"
    output=`$CLI $userSuffix -c "$sessionDeleteCmd"`
    echo "$output"
    echo "$output" | grep -q "deleted"
    local ret=$?
    return $ret
}


xcWhoami()
{
    local cmdName="whoami"
    cmdsTested[$cmdName]=1

    local whoamiCmd="$cmdName"

    echo "$whoamiCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$whoamiCmd"`
}

top()
{
    local cmdName="top"
    cmdsTested[$cmdName]=1

    local topCmd="$cmdName"

    echo "$topCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$topCmd"`
}

version()
{
    local cmdName="version"
    cmdsTested[$cmdName]=1

    local versionCmd="$cmdName"

    echo "$versionCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$versionCmd"`
}

loadAs()
{
    local userid=$1
    local format=$2
    local url=$3
    local size=$4
    local fieldDelim=$5
    local header=$6
    local recordDelim=$7
    local udf=$8
    local recursive=$9

    local cmdName="load"

    local randomDsName="cliTest-$RANDOM"

    cmdsTested[$cmdName]=1

    if [ "$userid" != "" ]; then
        local userSuffix="-u $userid"
    else
        local userSuffix=""
    fi

    local loadCmd="$cmdName --url \"$url\" --format \"$format\" --name \"$randomDsName\""
    if [ "$size" != "" ]; then
        loadCmd+=" --size \"$size\""
    fi
    if [ "$fieldDelim" != "" ]
    then
        loadCmd+=" --fielddelim \"$fieldDelim\""
    fi
    if [ "$header" = "1" ]
    then
        loadCmd+=" --header"
    fi
    if [ "$recordDelim" != "" ]
    then
        loadCmd+=" --recorddelim \"$recordDelim\""
    fi
    if [ "$udf" != "" ]
    then
        loadCmd+=" --apply \"$udf\""
    fi
    if [ "$recursive" != "" ]
    then
        loadCmd+=" --recursive"
    fi

    echo "$loadCmd;" >> "${cliCmdsOutputFile}${userid}"
    local cliOutput=`$CLI $userSuffix -c "$loadCmd"`
    echo "$cliOutput" | grep -q 'Loaded successfully into dataset'
    if [ "$?" != "0" ]
    then
        output="$CLI $userSuffix -c \"$loadCmd\" -> $cliOutput"
        return 1
    fi

    local datasetName=`echo "$cliOutput" | grep 'Loaded successfully into dataset' | cut -f2 -d':' | sed -e 's/^\s*//' -e 's/\s*$//'`

    output="$datasetName"

    return 0
}

# retinaMake retinaName tableName col1 alias1 col2 alias2 ...
retinaMake()
{
    local retinaName=$1
    local tableName=$2

    shift 2

    local cmdName="retina"
    local subCmd="make"

    cmdsTested[$cmdName]=1

    local numColumns=$#
    local input="1"

    if [ $(( $numColumns % 2 )) != "0" ]; then
        printInColor "red" "Column name or alias missing"
        return 1
    fi

    numColumns=$(( $numColumns / 2 ))
    input="$input\n$tableName"
    input="$input\n$numColumns"

    for ii in `seq 0 $numColumns`; do
        input="$input\n$1"
        shift
        input="$input\n$1"
        shift
    done

    local retinaMakeCmd="$cmdName $subCmd \"$retinaName\""
    echo "$retinaMakeCmd;" >> $cliCmdsOutputFile
    echo -e "$input" >> $TMP1/${retinaName}.tmp
    output=`echo -e "$input" | $CLI -c "$retinaMakeCmd"`
    echo "$output" | grep -q "created successfully"
    if [ "$?" != "0" ]; then
        return 1
    fi
    return 0
}

retinaSave()
{
    local retinaName=$1
    local retinaPath=$2
    local overwrite=$3

    local cmdName="retina"
    local subCmd="save"

    cmdsTested[$cmdName]=1

    local retinaSaveCmd="$cmdName $subCmd --retinaName \"$retinaName\" --retinaPath \"$retinaPath\""
    if [ "$overwrite" = "1" ]; then
        retinaSaveCmd="$retinaSaveCmd --force"
    fi
    echo "$retinaSaveCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$retinaSaveCmd"`
    echo "$output" | grep -q "saved successfully"
    if [ "$?" != "0" ]; then
        return 1
    fi
    return 0
}

retinaLoad()
{
    local retinaName=$1
    local retinaPath=$2

    local cmdName="retina"
    local subCmd="load"

    cmdsTested[$cmdName]=1

    local retinaLoadCmd="$cmdName $subCmd --retinaName \"$retinaName\" --retinaPath \"$retinaPath\""
    echo "$retinaLoadCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$retinaLoadCmd"`
    echo "$output" | grep -q "uploaded successfully"
    if [ "$?" != "0" ]; then
        return 1
    fi
    return 0
}

retinaExecute()
{
    local retinaName=$1
    local retinaParams=$2
    local dstTable=$3

    local cmdName="executeRetina"

    cmdsTested[$cmdName]=1

    local retinaRunCmd="$cmdName --retinaName \"$retinaName\""

    if [ $# -eq 2 ]; then
        retinaRunCmd="$retinaRunCmd --parameters $retinaParams"
    fi

    if [ $# -eq 3 ]; then
        retinaRunCmd="$retinaRunCmd --dstTable $dstTable"
    fi

    echo "$retinaRunCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$retinaRunCmd"`
    echo "$output" | grep -q "executed successfully"
    if [ "$?" != "0" ]; then
        return 1
    fi
    return 0
}

retinaDelete()
{
    local retinaName=$1

    local cmdName="retina"
    local subCmd="delete"

    cmdsTested[$cmdName]=1

    local retinaDeleteCmd="$cmdName $subCmd \"$retinaName\""
    echo "$retinaDeleteCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$retinaDeleteCmd"`
    echo "$output" | grep -q "deleted successfully"
    if [ "$?" != "0" ]; then
        return 1
    fi
    return 0
}

load()
{
    local format=$1
    local url=$2
    local size=$3
    local fieldDelim=$4
    local header=$5
    local recordDelim=$6
    local udf=$7
    local recursive=$8

    loadAs "" "$format" "$url" "$size" "$fieldDelim" "$header" "$recordDelim" "$udf" "$recursive"
    local ret=$?
    return $ret
}

handleTableError()
{
    local tableName=$1

    xinspect "$tableName"
    echo "$output" | grep -q "Error:"
    if [ "$?" = "0" ]; then
        echo "Dropping \"$tableName\" because of error"
        drop "$tableName"
    fi
}

getDag()
{
    local dagName=$1
    local cmdName="dag"
    cmdsTested[$cmdName]=1

    local dagCmd="$cmdName $dagName"

    echo "$dagCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$dagCmd"`

    echo "$output" | grep -q "error"
    ret=$?
    if [ $ret -eq 0 ]; then
        return 1
    else
        return 0
    fi
}

getSourceListing()
{
    local url=$1
    local cmdName="source"
    cmdsTested[$cmdName]=1

    local lsCmd="$cmdName --url $url --ls"

    echo "$lsCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$lsCmd"`

    echo "$output" | grep -q "error"
    ret=$?
    if [ $ret -eq 0 ]; then
        output="$CLI -c \"$lsCmd\" -> `echo $output`"
        return 1
    else
        return 0
    fi
}

uploadPython()
{
    local moduleName=$1
    local pythonPath=$2

    local cmdName="python"
    local subCmdName="upload"
    cmdsTested[$cmdName]=1

    local uploadPythonCmd="$cmdName $subCmdName --localFile \"$pythonPath\" --moduleName \"$moduleName\""
    echo "$uploadPythonCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$uploadPythonCmd"`
    echo "$output" | grep -q "uploaded successfully"
    if [ "$?" != "0" ]; then
        return 1
    fi
    return 0
}

listPython()
{
    local fnNamePattern=${1:-*}

    local cmdName="python"
    local subCmdName="list"
    cmdsTested[$cmdName]=1

    local listPythonCmd="$cmdName $subCmdName --fnNamePattern \"$fnNamePattern\""
    echo "$listPythonCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$listPythonCmd"`
}

downloadPython()
{
    local moduleName=$1
    local pythonPath=$2

    local cmdName="python"
    local subCmdName="download"
    cmdsTested[$cmdName]=1

    local downloadPythonCmd="$cmdName $subCmdName --localFile \"$pythonPath\" --moduleName \"$moduleName\""
    echo "$downloadPythonCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$downloadPythonCmd"`
    echo "$output" | grep -q "successfully downloaded"
    if [ "$?" != "0" ]; then
        return 1
    fi
    return 0
}

indexAs()
{
    local userid=$1
    local backingDataset=$2
    local key=$3
    local tableName=$4
    local sorted=$5
    local prefix=$6

    local cmdName="index"
    cmdsTested[$cmdName]=1

    if [ "$userid" = "" ]; then
        local userSuffix=""
    else
        local userSuffix="-u $userid"
    fi

    local indexCmd="$cmdName --prefix p --dataset \"$backingDataset\" --key \"$key\""
    indexCmd+=" --dsttable \"$tableName\""
    if [ "$sorted" = "true" ]; then
        indexCmd+=" --sorted"
    fi

    if [ "$prefix" != "" ]; then
        indexCmd+=" --prefix \"$prefix\""
    fi

    echo "$indexCmd;" >> "${cliCmdsOutputFile}${userid}"
    output=`$CLI $userSuffix -c "$indexCmd"`
    echo "$output" | grep -q "\"$tableName\" successfully created"
    if [ "$?" != "0" ]
    then
        return 1
    fi

    # User's responsibility to clean up after themselves if not running
    # with default user
    if [ "$userid" = "" ]; then
        tablesLoaded+="$tableName "
    fi

    return 0
}

index()
{
    local backingDataset=$1
    local key=$2
    local tableName=$3
    local sorted=$4

    indexAs "" "$backingDataset" "$key" "$tableName" "$sorted" "p"
    local ret=$?
    return $ret
}

indexPrefix()
{
    local backingDataset=$1
    local key=$2
    local tableName=$3
    local sorted=$4
    local prefix=$5

    indexAs "" "$backingDataset" "$key" "$tableName" "$sorted" "$prefix"
    local ret=$?
    return $ret
}

indexTable()
{
    local backingTableName=$1
    local key=$2
    local tableName=$3
    local sorted=$4

    local cmdName="index"
    cmdsTested[$cmdName]=1

    local indexCmd="$cmdName --srctable \"$backingTableName\" --key \"$key\""
    indexCmd+=" --dsttable \"$tableName\""
    if [ "$sorted" = "true" ]; then
        indexCmd+=" --sorted"
    fi

    echo "$indexCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$indexCmd"`
    echo "$output" | grep -q "\"$tableName\" successfully created"
    if [ "$?" != "0" ]
    then
        return 1
    fi

    tablesLoaded+="$tableName "

    return 0
}

xjoin()
{
    local leftTable=$1
    local rightTable=$2
    local tableName=$3

    local cmdName="join"
    cmdsTested[$cmdName]=1

    local joinCmd="$cmdName --leftTable \"$leftTable\" --rightTable \"$rightTable\""
    joinCmd+=" --joinTable \"$tableName\""

    echo "$joinCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$joinCmd"`
    echo "$output" | grep -q "\"$tableName\" successfully created"
    if [ "$?" != "0" ]
    then
        return 1
    fi

    tablesLoaded+="$tableName "

    return 0
}

xaggregateNoFilter()
{
    local srcTableName=$1
    local aggEvalStr=$2
    local dstTableName=$3

    local cmdName="aggregate"
    cmdsTested[$cmdName]=1

    local aggCmd="$cmdName --srctable \"$srcTableName\" --dsttable \"$dstTableName\" --eval \"$aggEvalStr\""

    echo "$aggCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$aggCmd"`
    if [ "`echo "$output" | cut -d: -f1`" = "Error" ]; then
        return 1
    fi

    tablesLoaded+="$dstTableName "
    return 0
}

xaggregate()
{
    local srcTableName=$1
    local aggEvalStr=$2
    local dstTableName=$3

    xaggregateNoFilter "$srcTableName" "$aggEvalStr" "$dstTableName"
    ret=$?
    if [ $ret -ne 0 ]; then
        return 1
    fi

    output=`echo "$output" | tail -1 | cut -d : -f 2 | tr -d ' '`
    return 0
}

xgroupBy()
{
    local srcTable=$1
    local aggEvalStr=$2
    local newFieldName=$3
    local dstTable=$4
    local nosample=$5
    local icv=$6

    local cmdName="groupBy"
    cmdsTested[$cmdName]=1

    local groupByCmd="$cmdName --srctable \"$srcTable\" --eval \"$aggEvalStr\" --fieldName \"$newFieldName\" --dsttable \"$dstTable\""
    if [ "$nosample" = "nosample" ]
    then
        groupByCmd="$groupByCmd --nosample"
    fi

    if [ "$icv" = "1" ]
    then
        groupByCmd="$groupByCmd --icv"
    fi

    echo "$groupByCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$groupByCmd"`
    echo "$output" | grep -q "\"$dstTable\" successfully created"
    if [ "$?" != "0" ]; then
        return 1
    fi

    tablesLoaded+="$dstTable "

    return 0
}

waitForQuery()
{
    local queryName=$1
    local cmdName="inspectquery"
    local numQueuedTasks=0
    local numRunningTasks=0
    local MaxNumTries=100
    local queryErrorStatus=""

    cmdsTested[$cmdName]=1

    local inspectQueryCmd="$cmdName \"$queryName\""
    echo "$inspectQueryCmd;" >> $cliCmdsOutputFile

    inspectQueryOutput=`$CLI -c "$inspectQueryCmd"`
    while read line; do
        label=`echo "$line" | cut -d: -f1`
        if [ "$label" = "Query state" ]; then
            queryState=`echo "$line" | cut -d: -f2 | sed -e 's/^ *//'`
        fi
        if [ "$label" = "Query error status" ]; then
            queryErrorStatus=`echo "$line" | cut -d: -f2 | sed -e 's/^ *//'`
        fi
    done <<< "$inspectQueryOutput"

    local numTries=0
    if [ "$queryState" = "qrNotStarted" -o "$queryState" = "qrProcessing" ]; then
        echo -n "Waiting for \"$queryName\""
    fi
    while [ "$queryState" = "qrNotStarted" -o "$queryState" = "qrProcessing" ]; do
        echo -n "."
        sleep 1
        inspectQueryOutput=`$CLI -c "$inspectQueryCmd"`
        while read line; do
            label=`echo "$line" | cut -d: -f1`
            if [ "$label" = "Query state" ]; then
                queryState=`echo "$line" | cut -d: -f2 | sed -e 's/^ *//'`
            fi
            if [ "$label" = "Query error status" ]; then
                queryErrorStatus=`echo "$line" | cut -d: -f2 | sed -e 's/^ *//'`
            fi
        done <<< "$inspectQueryOutput"
        numTries=$(( $numTries + 1 ))
        if [ $numTries -gt $MaxNumTries ]; then
            echo ""
            printInColor "red" "Timeout while waiting for query \"$queryName\""
            return 1
        fi
    done

    echo ""
    if [ "$queryState" = "qrError" ]; then
        printInColor "red" "Query failed with $queryErrorStatus"
        return 1
    fi
    return 0
}

xquery()
{
    local query=$1
    local queryNameTmp=$2
    local querySessionName=$3
    local cmdName="query"

    local escapedQuery=`echo "$query" | sed 's/"/\\\"/g'`
    cmdsTested[$cmdName]=1

    local queryCmd="$cmdName --xcquery \"$escapedQuery\" --queryname $queryNameTmp --sessionname $querySessionName"

    echo "$queryCmd;" >> $cliCmdsOutputFile
    queryOutput=`$CLI -c "$queryCmd"`
    echo "$queryOutput"
    label=`echo "$queryOutput" | cut -d: -f1 | sed -e 's/^ *//' | sed -e 's/ *$//'`
    if [ "$label" != "Query" ]; then
        printInColor "red" "Could not find label in $queryOutput"
        return 1;
    fi

    queryName=`echo "$queryOutput" | cut  -d: -f2 | sed -e 's/^ *//' | cut -d\  -f 1`
    echo "queryName $queryName"
    output="$queryName"
    echo "$queryOutput" | grep -q "$queryName successfully created"
    local ret=$?
    if [ "$ret" != "0" ]
    then
        printInColor "red" "Could not find $queryName"
        return 1
    fi

    return 0
}

xbatchQuery()
{
    printInColor "red" "NOT ALLOWED"
    exit 2
}

xfilter()
{
    local srcTable=$1
    local filterStr=$2
    local dstTable=$3

    local cmdName="filter"
    cmdsTested[$cmdName]=1

    local filterCmd="$cmdName --srctable \"$srcTable\" --eval \"$filterStr\" --dsttable \"$dstTable\""

    echo "$filterCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$filterCmd"`
    echo "$output"

    echo "$output" | grep -q "\"$dstTable\" successfully created"
    local ret=$?
    if [ "$ret" != "0" ]
    then
        local tmpOutput="$output"
        handleTableError "$dstTable"
        output="$tmpOutput"
        return 1
    fi

    tablesLoaded+="$dstTable "
    return 0
}

xproject()
{
    local srcTable=$1
    local dstTable=$2

    shift 2

    local cmdName="project"
    cmdsTested[$cmdName]=1

    local projectCmd="$cmdName --srctable \"$srcTable\" --dsttable \"$dstTable\" $@"

    echo "$projectCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$projectCmd"`
    echo "$output"

    echo "$output" | grep -q "\"$dstTable\" successfully created"
    local ret=$?
    if [ "$ret" != "0" ]
    then
        local tmpOutput="$output"
        handleTableError "$dstTable"
        output="$tmpOutput"
        return 1
    fi

    tablesLoaded+="$dstTable "
    return 0
}

xmap()
{
    local evalStr=$1
    local srcTable=$2
    local newFieldName=$3
    local dstTable=$4
    local icv=$5

    local cmdName="map"
    cmdsTested[$cmdName]=1

    local mapCmd="$cmdName --eval \"$evalStr\" --srctable \"$srcTable\" --fieldName \"$newFieldName\" --dsttable \"$dstTable\""

    if [ "$icv" = "1" ]
    then
        mapCmd="$mapCmd --icv"
    fi

    echo "$mapCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$mapCmd"`
    echo "$output"

    echo "$output" | grep -q "\"$dstTable\" successfully created"
    local ret=$?
    if [ "$ret" != "0" ]
    then
        local tmpOutput="$output"
        handleTableError "$dstTable"
        output="$tmpOutput"
        return 1
    fi

    tablesLoaded+="$dstTable "
    return 0
}

xgetRowNum()
{
    local srcTable=$1
    local newFieldName=$2
    local dstTable=$3

    local cmdName="getRowNum"
    cmdsTested[$cmdName]=1

    local getRowNumCmd="$cmdName --srctable \"$srcTable\" --fieldName \"$newFieldName\" --dsttable \"$dstTable\""

    echo "$getRowNumCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$getRowNumCmd"`
    echo "$output"

    echo "$output" | grep -q "\"$dstTable\" successfully created"
    local ret=$?
    if [ "$ret" != "0" ]
    then
        local tmpOutput="$output"
        handleTableError "$dstTable"
        output="$tmpOutput"
        return 1
    fi

    tablesLoaded+="$dstTable "
    return 0
}

xinspect()
{
    local tableName=$1

    local cmdName="inspect"
    cmdsTested[$cmdName]=1

    execCmd 0 "$cmdName" "" "$tableName"
}

inspectDataset()
{
    local dataset=$1

    local cmdName="inspect"
    cmdsTested[$cmdName]=1

    execCmd 0 "$cmdName" "" "--dataset $dataset"
}

getRowCount()
{
    local tableName=$1

    xinspect "$tableName"
    local inspectOutput="$output"
    local total=0
    for nodeCount in `echo "$inspectOutput" | grep 'Number of rows' | cut -f2 -d':'`
    do
        total=$(( $total + $nodeCount ))
    done
    output="$total"
}

listTablesAs()
{
    local userid=$1
    local cmdName="list"

    if [ "$userid" = "" ]; then
        local userSuffix=""
    else
        local userSuffix="-u $userid"
    fi

    cmdsTested[$cmdName]=1

    local listCmd="$cmdName tables"

    echo "$listCmd;" >> $cliCmdsOutputFile
    output=`$CLI $userSuffix -c "$listCmd"`

    echo "$output"
}

listTables()
{
    listTablesAs ""
    local ret=$?
    return $ret
}

listExportTargets()
{
    local targetType=$1
    local targetName=$2
    local cmdName="target"

    cmdsTested[$cmdName]=1

    local cmdString="$cmdName --list --type $targetType --name $targetName"
    echo "$cmdString;" >> $cliCmdsOutputFile
    output=`$CLI -c "$cmdString"`
    echo "$output"
}

getExportDir()
{
    listExportTargets "file" "Default"
    local line="$output"
    local exportDir=`echo "$output" | sed -n 2p | cut -f 3`
    output="${exportDir##nfs://}"
    echo "$output"
}

xexportSF()
{
    local cmdName="export"
    local format="$1"

    local cmdString="$cmdName"
    # These shift out all of the specific format args
    if [ "$format" = "csv" ]; then
        local fieldDelim="$2"
        local recordDelim="$3"
        shift
        shift
        if [ "$fieldDelim" != $'\t' ]; then
            cmdString="$cmdString --fieldDelim \"$fieldDelim\""
        fi
        if [ "$recordDelim" != $'\n' ]; then
            cmdString="$cmdString --recordDelim \"$recordDelim\""
        fi
    elif [ "$format" = "sql" ]; then
        local sqlTableName="$2"
        local dropTable="$3"
        local createTable="$4"
        shift
        shift
        shift
        cmdString="$cmdString --sqlTableName \"$sqlTableName\""
        if [ "$dropTable" != "0" ]; then
            cmdString="$cmdString --dropTable"
        fi
        if [ "$createTable" != "0" ]; then
            cmdString="$cmdString --createTable"
        fi
    fi

    local splitType=$2
    # These shift out all of the split type specific args
    if [ "$splitType" = "size" ]; then
        local maxFileSize="$3"
        shift
        cmdString="$cmdString --maxFileSize \"$maxFileSize\""
    fi

    local headerType=$3
    local fileName=$4
    local tableName=$5
    local createRule=$6
    local columnNames=$7
    local headerColumnNames=$8


    local targetType="file"
    local targetName="Default"
    getExportDir
    local targetDir="$output"
    local exportDir="${fileName%.*}"
    local exportPath="$targetDir/$exportDir"

    cmdsTested[$cmdName]=1

    local cmdString="$cmdString --targetType \"$targetType\" --tableName \"$tableName\" --format \"$format\" --splitRule \"$splitType\" --headerType \"$headerType\" --fileName \"$fileName\" --targetName \"$targetName\" --createRule \"$createRule\" --columnNames \"$columnNames\" --headerColumnNames \"$headerColumnNames\" "
    echo "$cmdString;" >> "$cliCmdsOutputFile"
    output="$($CLI -c "$cmdString")"
    echo "$output"
    echo "$output" | grep -q "Export Successful"
    local ret=$?
    if [ $ret -ne 0 ]; then
        return $ret
    fi

    output="$exportPath"
    return 0
}

dropAll()
{
    local cmdName="drop"
    local tables
    local numTables
    local tablesFound
    local numTablesFound=0
    local dropNumTables
    local tableName
    local status
    local error=0
    local tableExists

    cmdsTested[$cmdName]=1

    read -a tables <<< "$tablesLoaded"
    numTables=${#tables[@]}

    for ii in `seq 0 $(( $numTables - 1 ))`; do
        tablesFound[$ii]=0
    done

    echo "$cmdName export *;" >> $cliCmdsOutputFile
    output=`$CLI -c "$cmdName export *;"`

    echo "$cmdName table *; $cmdName constant *" >> $cliCmdsOutputFile
    output=`$CLI -c "$cmdName table *; $cmdName constant *"`
    oldIFS=$IFS
    IFS=$'\n'
    totalDroppedNode=0
    for line in $output; do
        case $line in
        "Number of "*)
            dropNumTables=`cut -d : -f 2 <<< "$line" | tr -d ' '`
            totalDroppedNode=$(($dropNumTables + $totalDroppedNode))
            ;;
        *)
            tableName=`cut -f 1 <<< "$line"`
            # Strip end quote
            tableName="${tableName%\"}"
            # Strip start quote
            tableName="${tableName#\"}"

            status=`cut -f 2 <<< "$line"`
            # Strip end quote
            status="${status%\"}"
            # Strip start quote
            status="${status#\"}"

            if [ "$status" != "XCE-00000000 Success" ]; then
                printInColor "red" "Table \"$tableName\" was not dropped (status returned: $status)"
                error=1
            fi

            tableExists=0
            for ii in `seq 0 $(( $numTables - 1 ))`; do
                if [ "$tableName" = "${tables[$ii]}" ]; then
                    if [ ${tablesFound[$ii]} -eq 0 ]; then
                        tableExists=1
                        tablesFound[$ii]=1
                        numTablesFound=$(( $numTablesFound + 1 ))
                        break
                    else
                        printInColor "red" "Table \"$tableName\" was dropped more than once"
                        error=1
                    fi
                fi
            done;
            if [ $tableExists -ne 1 ]; then
                printInColor "red" "Unaccounted table \"$tableName\""
                error=1
            fi
            ;;
        esac
    done

    if [ $totalDroppedNode -ne $numTables ]; then
        printInColor "red" "Number of tables dropped ($totalDroppedNode) does not match number of tables loaded ($numTables)"
        error=1
    fi

    IFS=$oldIFS

    if [ $numTablesFound -ne $numTables ]; then
        for ii in `seq 0 $(( $numTables - 1 ))`; do
            if [ ${tablesFound[$ii]} -eq 0 ]; then
                printInColor "red" "\"${tables[$ii]}\" was not dropped"
            fi
        done
        error=1
    fi

    if [ $error -eq 0 ]; then
        for ii in `seq 0 $(( $numTables - 1 ))`; do
            echo "Dropped table \"${tables[$ii]}\""
        done
        tablesLoaded=""
    fi

    return $error
}

dropAs()
{
    local userid=$1
    local tableName=$2
    local cmdName="drop"

    cmdsTested[$cmdName]=1

    if [ "$userid" = "" ]; then
        local userSuffix=""
    else
        local userSuffix="-u $userid"
    fi

    echo "$cmdName table $tableName;" >> "${cliCmdsOutputFile}${userid}"
    output=`$CLI $userSuffix -c "$cmdName table \"$tableName\""`
    echo "$output"
    echo "$output" | grep -q "Error:"
    local ret=$?
    if [ "$ret" = "0" ]; then
        return 1
    fi

    lineNum="0"
    while read line; do
        echo $line
        if [ "$lineNum" = "0" ]; then
            numTablesDeleted=`echo "$line" | cut -d: -f2`
        else
            statusMsg=`echo "$line" | cut -f2`
            echo "$statusMsg" | grep -q "Success"
            local ret=$?
            if [ "$ret" != "0" ]; then
                return 1
            fi
        fi

        lineNum=$(( $lineNum + 1 ))
    done<<<"$output"
}

drop()
{
    local tableName=$1

    dropAs "" "$tableName"
    local ret=$?
    return $ret
}

dropLoadNodeAs()
{
    local userid=$1
    local dataset=$2
    local cmdName="drop"

    cmdsTested[$cmdName]=1

    if [ "$userid" = "" ]; then
        local userSuffix=""
    else
        local userSuffix="-u $userid"
    fi

    echo "$cmdName dataset $dataset;" >> "${cliCmdsOutputFile}${userid}"
    output=`$CLI $userSuffix -c "$cmdName dataset \"$dataset\""`
    echo "$output"
    echo "$output" | grep -q "Success"
    local ret=$?
    return $ret
}

dropLoadNode()
{
    local dataset=$1

    dropLoadNodeAs "" "$dataset"
    local ret=$?
    return $ret
}

delistDs()
{
    local cmdName="delist"
    local datasetNamePattern=$1

    cmdsTested[$cmdName]=1

    local delistDsCmd="$cmdName \"$datasetNamePattern\""
    echo "$delistDsCmd;" >> $cliCmdsOutputFile
    output=`$CLI -c "$delistDsCmd"`
    echo "$output"
    echo "$output" | grep -q "Error:"
    local ret=$?
    if [ "$ret" = "0" ]; then
        return 1
    fi

    lineNum="0"
    while read line; do
        echo $line
        if [ "$lineNum" = "0" ]; then
            numTablesDeleted=`echo "$line" | cut -d: -f2`
        else
            statusMsg=`echo "$line" | cut -f2`
            echo "$statusMsg" | grep -q "Success"
            local ret=$?
            if [ "$ret" != "0" ]; then
                return 1
            fi
        fi

        lineNum=$(( $lineNum + 1 ))
    done<<<"$output"
}

dropAllDS()
{
    local cmdName="list"

    cmdsTested[$cmdName]=1

    local ret=0
    local inspectDsOutput=`$CLI -c "$cmdName datasets"`

    while read line; do
        local dataset=`echo "$line" | cut -f2`
        # Strip end quote
        dataset="${dataset%\"}"
        # Strip start quote
        dataset="${dataset#\"}"
        if [ "$dataset" = "" ]; then
            continue
        fi
        dropLoadNode $dataset
        ret=$?
        if [ "$ret" != "0" ]
        then
            echo "failed to drop load node \"$dataset\"" 1>&2
            return $ret
        fi

        delistDs "$dataset"
        ret=$?
        if [ "$ret" != "0" ];
        then
            echo "Failed to delist dataset \"%dataset\"" 1>&2
            return $ret
        fi
    done<<<"`echo "$inspectDsOutput" | tail -n+3`"

    return 0
}

validateTableExists()
{
    local tableName=$1
    local indexCompleted=0
    local refCount=""

    echo "Validating $tableName" 1>&2
    execCmd 0 "inspect" "" "\"$tableName\""
    refCount=`echo "$output" | grep "Reference count:"`
    ret=$?
    if [ $ret -ne 0 ]; then
        echo "$output" 1>&2
        return $ret
    fi

    return 0
}

loadDataset()
{
    local datasetUrl=$1
    local datasetType=$2
    local baseTableName=$3
    local key=$4
    shift 4

    local cmdName="load"
    cmdsTested[$cmdName]=1

    local tableName
    local tablesIndexed=""

    # First load the dataset
    local dataset
    load "$datasetType" "$datasetUrl"
    ret=$?
    dataset="$output"
    if [ $ret -ne 0 ] || [ "$dataset" = "" ]; then
        return 1
    fi

    local backingTable="$baseTableName-$key"

    index $dataset "$key" "$backingTable"
    ret=$?
    if [ $ret -ne 0 ]; then
        return 1
    fi

    index $dataset "$key" "$backingTable-sorted" "true"
    ret=$?
    if [ $ret -ne 0 ]; then
        return 1
    fi

    # Now the rest of the index can happen in parallel
    while [ $# -gt 0 ]; do
        key=$1
        shift

        tableName="$baseTableName-$key"
        index $dataset "$key" "$tableName"
        ret=$?
        if [ $ret -ne 0 ]; then
            return 1
        fi

        tablesIndexed+="$tableName "

        # Do a sorted index
        tableName="$baseTableName-$key-sorted"
        index $dataset "$key" "$tableName" "true"
        ret=$?
        if [ $ret -ne 0 ]; then
            return 1
        fi

        tablesIndexed+="$tableName "
    done

    output="$dataset"
    return 0
}

execCmd()
{
    local timeout=$1
    local cmdName=$2
    local userid=$3
    local tempOut
    local cliPid
    local startTime

    cmdsTested[$cmdName]=1

    if [ "$3" = "" ]; then
        local userSuffix=""
    else
        local userSuffix="-u $userid"
    fi

    shift 3

    local cmdInput="$cmdName $@"
    if [ "$timeout" -gt 0 ]; then
        tempOut=`mktemp $DIR/tempOut.XXXXX`
        echo "$cmdInput" >> $cliCmdsOutputFile
        $CLI $userSuffix -c "$cmdInput" > "$tempOut" &
        cliPid=$!
        startTime=0
        while [ $startTime -lt $timeout ]; do
            jobStatus=`jobs -l | grep $cliPid | awk '{print $3}'`
            if [ "$jobStatus" != "Running" ]; then
                output=`cat "$tempOut"`
                wait $cliPid
                ret=$?

                if [ $ret -ne 0 ]; then
                    printInColor "red" "Error executing $cmdName. Check $tempOut"
                else
                    rm "$tempOut"
                fi

                return $ret
            fi
            sleep 1
            startTime=$(( $startTime + 1 ))
        done
        output="Command timed out"
        echo "Command timed out"
        return $statusErrorTimeout
    else
        echo "$cmdInput;" >> "${cliCmdsOutputFile}${userid}"
        output=`$CLI $userSuffix -c "$cmdInput"`
    fi

    return 0
}

getStat()
{
    local nodeId=$1
    local statGroup=$2
    local statName="$3"

    local result=$($CLI -c "stats $nodeId $statGroup" | grep "$statName")
    if [ ! -z "$result" ]; then
        local resultArr=($result)
        output="${resultArr[${#resultArr[@]} - 1]}"
    else
        output=""
    fi
}

getJsonRecord()
{
    local rawInput=$1
    local row=`echo "$rawInput" | sed -n '3p'`
    echo "$row"
}

flattenArrayValue()
{
    local fatArrayValue="$1"
    local flattenValue=""

    while read line; do
        if [ "$line" = "[" -o "$line" = "]" ]; then
            continue;
        fi

        flattenValue="$flattenValue$line"
    done <<< "$fatArrayValue"

    echo "$flattenValue"
}

getRetinaOutputTables()
{
    local retinaPath=$1
    local retinaInfoPath="$retinaPath/dataflowInfo.json"

    # Look for retinainfo.json in case this is a older Dataflow file
    if [ -f "$retinaPath/retinaInfo.json" ]; then
        retinaInfoPath="$retinaPath/retinaInfo.json"
    fi

    local jqCmd='.tables | length'
    local numTables=`cat $retinaInfoPath | jq "$jqCmd"`

    for ii in `seq 0 $numTables`; do
        local jqCmd=".tables[$ii].name"
        local tableName=`cat $retinaInfoPath | jq "$jqCmd"`
        # Strip end quote
        tableName="${tableName%\"}"
        # Strip start quote
        tableName="${tableName#\"}"
        retinaOutputTables[$ii]="$tableName"
    done

    numRetinaOutputTables=$numTables
}

getJsonType()
{
    local jsonRecord=$1
    local jsonField=$2

    echo "$jsonRecord" | jq -M -a ".$jsonField | type" | sed -e 's/^"//' -e 's/"$//'
}

getJsonField()
{
    local jsonRecord=$1
    local jsonField=$2

    local oldIFS=$IFS
    IFS=.
    local jsonFieldSelector="."
    for field in $jsonField; do
        jsonFieldSelector="$jsonFieldSelector[\"$field\"]"
    done
    IFS=$oldIFS

    # JQ cant handle large numbers (https://github.com/stedolan/jq/issues/369)
    # Hence this funky workaround
    echo "$jsonRecord" | jq -M -a "$jsonFieldSelector | type" 2>$TMP1/getJsonField.stderr 1>$TMP1/getJsonField.stdout
    local ret=$?
    local jsonError="`cat $TMP1/getJsonField.stderr`"
    local jsonFieldType="`cat $TMP1/getJsonField.stdout | sed -e 's/^"//' -e 's/"$//'`"

    if [ "$ret" -ne 0 ]; then
        echo "getJsonField() failed: $jsonError" 1>&2
        return $ret
    fi

    if [ "$jsonFieldType" = "number" ]; then
        local jsonNumber=`echo "$jsonRecord" | jq -M -a "$jsonFieldSelector" | sed -e 's/^"//' -e 's/"$//'`

        # JQ sometimes emit numbers in scientific notation (https://github.com/stedolan/jq/issues/218)
        echo "$jsonNumber" | grep -q "e"
        ret=$?
        if [ $ret -eq 0 ]; then
            jsonNumber=`printf "%f" "$jsonNumber"`
        fi

        if [ `echo "$jsonNumber < 0" | bc -l` -eq 1 ]; then
            local absJsonNumber=`echo "$jsonNumber * -1" | bc -l`
        else
            local absJsonNumber=$jsonNumber
        fi
        if [ `echo "$absJsonNumber > 1000000000" | bc -l` -eq 1 ]; then
            echo "$jsonRecord" | python -mjson.tool | grep "$jsonField" | cut -d: -f2 | sed -e 's/,\s*$//' -e 's/^\s*//' -e 's/\s*$//'
        else
            echo "$jsonNumber"
        fi
    elif [ "$jsonFieldType" = "array" ]; then
        echo "$jsonRecord" | jq -M -a "$jsonFieldSelector"
    else
        echo "$jsonRecord" | jq -M -a "$jsonFieldSelector" | sed -e 's/^"//' -e 's/"$//'
    fi
}

getJsonListOfFields()
{
    local jsonRecord="$1"
    local jsonListOfFields=""

    while read line; do
        if [ "$line" = "[" -o "$line" = "]" ]; then
            continue
        fi

        local jsonField=`echo "$line" | sed -e 's/^"//' -e 's/",$//' -e 's/"$//'`
        local jsonType=`echo "$jsonRecord" | jq -M -a ".$jsonField | type" | sed -e 's/^"//' -e 's/"$//'`

        if [ "$jsonType" = "object" ]; then
            getJsonListOfFields "`echo \"$jsonRecord\" | jq -M -a \".$jsonField\"`"
            local nestedListOfFields="$output"
            local field
            for field in $nestedListOfFields; do
                if [ "$jsonListOfFields" = "" ]; then
                    jsonListOfFields="${jsonField}.$field"
                else
                    jsonListOfFields="$jsonListOfFields"$'\t'"${jsonField}.$field"
                fi
            done
        else
            if [ "$jsonListOfFields" = "" ]; then
                jsonListOfFields="$jsonField"
            else
                jsonListOfFields="$jsonListOfFields"$'\t'"$jsonField"
            fi
        fi
    done <<< "`echo \"$jsonRecord\" | jq -M -a 'keys'`"

    output="$jsonListOfFields"
}

getNumFieldsInJsonRecord()
{
    local jsonRecord=$1
    echo "$jsonRecord" | jq -M -a 'length'
}

floatEq()
{
    local left=$1
    local right=$2
    local threshold="0.000001"

    read -d '' pythonScript << EOF
import math;

try:
    print (abs(($left) - ($right)) < $threshold);
except:
    print ("$left" == "$right");
EOF

    [ $(echo "$pythonScript" | python) = "True" ];
}
