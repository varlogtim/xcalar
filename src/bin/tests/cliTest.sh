#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`

echo "*** cliTest.sh STARTING ON `/bin/date`"

# Constants you can modify
numNodes=3
pathToQaDatasets=$(readlink -f "$BUILD_DIR/src/data/qa")

# Determine config file.
configFile="${XCE_CONFIG:-$XLRDIR/src/data/${HOSTNAME}.cfg}"
if [ ! -f "$configFile" ]; then
    configFile="$XLRDIR/src/bin/usrnode/test-config.cfg"
fi
apiPort=`grep Node.0.ApiPort $configFile | cut -f2 -d=`
myPid=$$
CLI="xccli --noprettyprint -o $apiPort -k $myPid"
cliTestSessionName="cliTest"

# set up alternate CLI command to target node 1
apiPortN1=`grep Node.1.ApiPort $configFile | cut -f2 -d=`
myPidN1=99999
CLIN1="xccli --noprettyprint -o $apiPortN1 -k $myPidN1"
cliTestSessionNameN1="cliTestNode1"

. utils.sh
. qa.sh

# List of checks you want
addTestCase "preChecks" preChecks $TestCaseEnable "362"
addTestCase "basicChecks" basicChecks $TestCaseEnable "144"

. $DIR/cliTestSuites/dagTests.sh
addTestCase "list datasets on non-default usrnode" listDatasetsFromNonZeroNode $TestCaseDisable "2460"

. $DIR/cliTestSuites/indexTests.sh
addTestCase "index fields of varying types" indexVaryingTypes $TestCaseEnable "176"
addTestCase "index non-ascii test" indexNonAsciiTest $TestCaseEnable ""

. $DIR/cliTestSuites/correlationTest.sh
# depends on basicChecks() table
addTestCase "correlationTest" correlationTest $TestCaseEnable
addTestCase "correlation0Test" correlation0Test $TestCaseEnable "2175"

. $DIR/cliTestSuites/userTests.sh
addTestCase "Private tables test" privateTablesTest $TestCaseEnable "2687"
addTestCase "Private tables test2" privateTablesTest2 $TestCaseEnable ""
addTestCase "Dataset shared test" datasetSharedTest $TestCaseEnable "4986"

addTestCase "dropTablesCheckpoint" dropTablesCheckpoint $TestCaseEnable

addTestCase "cleanup" cleanup $TestCaseEnable "60 98"

tablesLoaded=""

. nodes.sh

declare -A cmdsTested
. $DIR/cliCmds.sh

export TMP1=`mktemp /tmp/cliTest.XXXXX`
rm -rf $TMP1

mkdir $TMP1
echo "Creating cliTest output in $TMP1"

cliCmdsOutputFile="$TMP1/cmdOutput.log"
echo "CLI commands invoked can be found in $cliCmdsOutputFile"

verifyListTables()
{
    local tables
    local numTables
    local numTablesFound=0
    local listNumTables
    local tableName
    local tableExists
    local error=0
    local tablesFound
    local tmpList=`mktemp /tmp/cliTest.XXXXX`
    rm -f $tmpList

    read -a tables <<< "$tablesLoaded"
    numTables=${#tables[@]}

    for ii in `seq 0 $(( $numTables - 1 ))`; do
        tablesFound[$ii]=0
    done

    local cmdName="list"
    local xccli="$CLI -c"
    $xccli "$cmdName table *; $cmdName constant *;" > $tmpList
    result=`cat $tmpList`
    echo "$result"
    rm -f $tmpList

    oldIFS=$IFS
    IFS=$'\n'
    totalNumberNode=0
    for line in $result; do
        case $line in
        "Number of tables"*)
            listNumTables=`cut -d : -f 2 <<< "$line" | tr -d ' '`
            totalNumberNode=$(($listNumTables + totalNumberNode))
            ;;
        "Table ID"*)
            # Do nothing
            ;;
        *)
            tableName=`cut -f 2 <<< "$line"`
            # Strip end quote
            tableName="${tableName%\"}"
            # Strip start quote
            tableName="${tableName#\"}"

            tableExists=0
            for ii in `seq 0 $(( $numTables - 1 ))`; do
                if [ "$tableName" = "${tables[$ii]}" ]; then
                    if [ ${tablesFound[$ii]} -eq 0 ]; then
                        tableExists=1
                        tablesFound[$ii]=1
                        numTablesFound=$(( $numTablesFound + 1 ))
                        break
                    else
                        printInColor "red" "Table \"$tableName\" appeared more than once"
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

    if [ $totalNumberNode -ne $numTables ]; then
        printInColor "red" "List output ($totalNumberNode) does not match numTables ($numTables)"
        error=1
    fi

    IFS=$oldIFS

    if [ $numTablesFound -ne $numTables ]; then
        for ii in `seq 0 $(( $numTables - 1 ))`; do
            if [ ${tablesFound[$ii]} -eq 0 ]; then
                printInColor "red" "Did not find table \"${tables[$ii]}\""
            fi
        done
        error=1
    fi

    return $error
}

dropTablesCheckpoint()
{
    verifyListTables
    ret=$?
    if [ $ret -ne 0 ]; then
        return $ret
    fi

    dropAll
    ret=$?
    if [ $ret -ne 0 ]; then
        return $ret
    fi

    dropAllDS
    ret=$?
    if [ $ret -ne 0 ]; then
        return $ret
    fi

    return 0
}

getListOfCmds()
{
    local helpOutput

    execCmd 0 "help" ""
    helpOutput=$output

    oldIFS=$IFS
    IFS=$'\n'
    for cmd in $helpOutput; do
        case $cmd in
        "List of commands available"*)
            # Do nothing
            ;;
        *)
            cmdToTest=`cut -d - -f 1 <<< "$cmd" | tr -d ' '`
            if [ "${cmdsTested[$cmdToTest]}" = "1" ]; then
                cmdsTested[$cmdToTest]=2
            else
                cmdsTested[$cmdToTest]=0
            fi
            ;;
        esac
    done;
    IFS=$oldIFS
}

# Basically checks if our testing infrastructure is working properly
preChecks()
{
    local negNumber=-5000000000000
    local jsonRecord="{\"negNumber\": $negNumber}"
    local jsonNumber=`getJsonField "$jsonRecord" "negNumber"`

    if [ "$jsonNumber" != "$negNumber" ]; then
        printInColor "red" "$jsonNumber != $negNumber"
        return 1
    fi

    # Let's start a new session
    sessionNewOrReplace "" "$cliTestSessionName"

    local ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not create session \"$cliTestSessionName\""
        return $ret
    fi

    sessionActivate "" "$cliTestSessionName"

    local ret2=$?
    if [ "$ret2" != "0" ]; then
        printInColor "red" "Could not activate session \"$cliTestSessionName\""
        return $ret2
    fi

    return 0
}

basicChecks()
{
    local datasetUrl1="nfs://$pathToQaDatasets/yelp/user/yelp_academic_dataset_user_fixed.json"
    local datasetUrl2="nfs://$pathToQaDatasets/yelp/tip"
    local datasetType="json"
    local baseTableName1="yelp/user"
    local baseTableName2="yelp/tip"
    local tableName
    # user_id - string
    # votes.funny - integer
    # average_stars - float
    # name - contains string of varying length
    local keys=("user_id" "votes.funny" "average_stars" "name")

    # run top and version; we don't validate their output, but at least
    # it gets run to do a basic check
    top
    version

    xcWhoami
    xcCliUserId=`echo "$output" | cut -d\  -f1`
    if [ "$xcCliUserId" != "`whoami`" ]; then
        printInColor "red" "`whoami` != $xcCliUserId"
        return 1
    fi

    loadDataset "$datasetUrl1" "$datasetType" "$baseTableName1" ${keys[@]}
    local ret=$?
    dataset1="$output"
    if [ $ret -ne 0 ] || [ "$dataset1" = "" ]; then
        return 1
    fi

    loadDataset "$datasetUrl2" "$datasetType" "$baseTableName2" ${keys[0]}
    local ret=$?
    dataset2="$output"
    if [ $ret -ne 0 ] || [ "$dataset2" = "" ]; then
        return 1
    fi

    echo "PASS" 1>&2
    return 0
}

cleanup()
{
    verifyListTables
    ret=$?
    if [ $ret -ne 0 ]; then
        return $ret
    fi

    dropAll
    ret=$?
    if [ $ret -ne 0 ]; then
        return $ret
    fi

    dropAllDS
    ret=$?
    if [ $ret -ne 0 ]; then
        return $ret
    fi

    # Terminate our current session
    sessionInact "" "$cliTestSessionName"
    ret=$?
    if [ "$ret" != "0" ]; then
        printInColor "red" "Could not terminate session \"$cliTestSessionName\""
        return $ret
    fi

    # Delete all sessions
    sessionDelete "" "*"

    echo "Shutdown cluster"
    xc2 cluster stop
    exitCode=$?

    return $exitCode
}

forcefulExit()
{
    exitCode=$1
    murderNodes
    for filn in `/bin/ls -1 $TMP1`
    do
        grep -q -i assert $TMP1/$filn
        if [ "$?" = "0" ]
        then
            echo "*** assertion failure detected $filn: `grep -i assert $TMP1/$filn`"
        fi
    done
    echo "*** Leaving cliTest output in $TMP1 for your examination"
    echo "1..$currentTestNumber"
    echo "*** If one of your usrnodes crashed, find the core file in $XLRDIR/src/bin/tests/"
    exit $exitCode
}

# Now we're off to a clean start
echo "Shutdown cluster"
xc2 cluster stop

echo "Start cluster"
xc2 cluster start --num-nodes $numNodes

echo "Run test suite"
runTestSuite

printInColor "green" "All tests succeeded!"

echo "$TMP1"
# rm -rf $TMP1

echo "1..$currentTestNumber"
echo "*** cliTest.sh Ending On `/bin/date`"

exit 0
