#!/bin/bash

XcalarQaMode="daemon"
XcalarQaNumNodes=1

pathToQaDatasets=$BUILD_DIR/src/data/qa
YelpUserPath=$pathToQaDatasets/yelp/user/yelp_academic_dataset_user_fixed.json
YelpUserPathSL="$YelpUserPath-sl"
SessionReplayUDFPath=$pathToQaDatasets/sessionReplay/test.py
SessionReplayUDFPathSL="$SessionReplayUDFPath-sl"
pathToUsrNode=$XLRDIR/src/bin/usrnode

UsrUniqueID=1984
UsrNameGlobal="xcalar"
# This is the expected number of rows in the resultant table to ensure that
# the operation succeeded.
ExpectedNumberOfRows=70817
SessionId=0

Hostname=`hostname`
configFile="`readlink -f $pathToUsrNode/test-config.cfg`"
hostFile="`readlink -f $XLRDIR/src/data/${Hostname}.cfg`"
if [ -e "$hostFile" ]; then
    configFile="$hostFile"
fi

echo "Config file: $configFile"

DHTName="votesFunnyDht"
rootPath=` cat $configFile  | grep Constants.XcalarRootCompletePath | awk -F'=' '{print $2}'`
echo "rootPath is" $rootPath
persistedFilePath="$rootPath"
sessionReplayData="/netstore/qa/sessionReplayTest"

. qa.sh
. nodes.sh
source $XLRDIR/doc/env/xc_aliases

rm -f $YelpUserPathSL
rm -f $SessionReplayUDFPathSL
ln -s $YelpUserPath $YelpUserPathSL
ln -s $SessionReplayUDFPath $SessionReplayUDFPathSL

exitHandler()
{
    if [[ ! -z "$GuardRailsArgs" ]]; then
        rm -f grargs.txt
    fi
}

trap exitHandler EXIT

forcefulExit()
{
    exitCode=$1
    murderNodes
    echo "1..$currentTestNumber"
    echo "*** If one of your usrnodes crashed, find the core file in $XLRDIR/src/bin/tests/"
    exit $exitCode
}

cliCmd()
{
    local usrname=""
    local cmd=""

    if [ $# -eq 2 ]; then
        usrname=$1
        cmd=$2
    else
        usrname=$UsrNameGlobal
        cmd=$1
    fi

    xccli -u $usrname -k $UsrUniqueID -c "$cmd"
}

#
# Create a session with some useful operations.
#
constructSession()
{
    local usrname=$1
    local sessionName=$2
    local numUsrNode=$3
    local sessionNewOut

    echo "Constructing session: usrname is $usrname"
    echo "Constructing session: sessionName is $sessionName"
    echo "Constructing session: numUsrNode is $numUsrNode"

    xc2 -vv cluster start --num-nodes ${numUsrNode:-${XcalarQaNumNodes}}
    local ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi
    sessionNewOut=`cliCmd $usrname "session --new --name $sessionName"`
    if [ "$sessionNewOut" == "0" ]; then
        return 1
    fi
    echo $sessionNewOut
    SessionId=`echo $sessionNewOut|awk -F'=' '{print $2}'|awk '{print $1}'`
    cliCmd $usrname "session --switch --name $sessionName" || return 1
    cliCmd $usrname "python upload --localFile $SessionReplayUDFPath --moduleName sessionReplayTest" || return 1
    cliCmd $usrname "dht create --dhtName $DHTName --upperBound 25159 --lowerBound 0" || return 1
    cliCmd $usrname "load --url nfs://$YelpUserPathSL --format json --name yelpUser --size 1GB" || return 1
    cliCmd $usrname "index --key votes.funny --dataset .XcalarDS.yelpUser --dhtname $DHTName --dsttable dhtFunnyTable --prefix p" || return 1
    cliCmd $usrname "map --eval sessionReplayTest:nonOp(p::votes.useful) --srctable dhtFunnyTable --fieldName nonOp --dsttable nonOpTable" || return 1

    cliCmd $usrname "inspect nonOpTable" | grep $ExpectedNumberOfRows
    if [ $? -ne 0 ]; then
        echo "fail to build the initial session"
        return 1
    fi

    cliCmd $usrname "session --persist --name $sessionName" || return 1

    return 0
}

#
# Ensure successful session list and activate after an orderly shutdown.
# Note that the session activation does NOT replay the query graph created
# in the session in "constructSession" before the shutdown. So, the test
# shouldn't check for the tables or other data created before the shutdown
#
gracefullyShutdown()
{
    local username="gracefullyShutDownUsr"
    local sessionName="gracefullyShutDownSession"
    local ret=0

    constructSession $username $sessionName
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    xc2 cluster stop
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    xc2 -vv cluster start --num-nodes ${XcalarQaNumNodes}
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    cliCmd $username "session --list" || return 1
    cliCmd $username "session --switch --name $sessionName" || return 1
    if [ $? -ne 0 ]; then
        echo "failed to activate session"
        return 1
    fi

    return 0
}

#
# Ensure successful replay after usrnode is killed (unclean shutdown).
#
killedUsrnodeTest()
{
    local username="killedUsrnodeUsr"
    local sessionName="killedUsrnodeUsrSession"
    local ret=0

    constructSession $username $sessionName
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    murderNodes
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    xc2 -vv cluster start --num-nodes ${XcalarQaNumNodes}
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    cliCmd $username "session --list" || return 1
    cliCmd $username "session --switch --name $sessionName" || return 1
    if [ $? -ne 0 ]; then
        echo "fail to activate session"
        return 1
    fi

    return 0
}

#
# Check for replay failure after required Dht is renamed.  Then check for
# successful replay after the Dht is renamed back to the original name.
#
missingDHTTest()
{
    local username="missingDHTUsr"
    local sessionName="missingDHTSession"
    local ret=0
    local errorMsg="Error: server returned: Dht specified does not exist"

    constructSession $username $sessionName
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    xc2 cluster stop
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    # Rename the DHT file
    mv "$persistedFilePath/sessions/Xcalar.DHT.$DHTName" "$persistedFilePath/sessions/Xcalar.DHT.$DHTName.save"
    if [ $? -ne 0 ]; then
        echo "Failed to rename DHT file"
        return 1
    fi

    xc2 -vv cluster start --num-nodes ${XcalarQaNumNodes}
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    # Session replay is expected to fail due to the missing DHT
    cliCmd $username "session --list" || return 1
    cliCmd $username "session --switch --name $sessionName" | grep "$errorMsg"
    if [ $? -ne 0 ]; then
        echo "error msg does not match"
        return 1
    fi

    xc2 cluster stop
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    # Rename the DHT file back to its original name
    mv "$persistedFilePath/sessions/Xcalar.DHT.$DHTName.save" "$persistedFilePath/sessions/Xcalar.DHT.$DHTName"
    if [ $? -ne 0 ]; then
        echo "Failed to rename DHT file"
        return 1
    fi

    xc2 -vv cluster start --num-nodes ${XcalarQaNumNodes}
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    # Session replay is expected to succeed as the DHT is in the expected location
    cliCmd $username "session --list" || return 1
    cliCmd $username "session --switch --name $sessionName" || return 1

    cliCmd $username "inspect nonOpTable" | grep $ExpectedNumberOfRows
    if [ $? -ne 0 ]; then
        echo "failed to replay session"
        return 1
    fi

    return 0
}

#
# Constructs a session on a cluster with the initial number of nodes.  Then
# an orderly shutdown is done and a cluster with a different number of nodes
# is started.  Then ensure a successful replay occurs.
#
replayOnDiffNumUsrNode()
{
    local initNumUsrNode=$1
    local replayNumUsrNode=$2

    local username=$3
    local sessionName=$4
    local ret=0

    # Create a cluster with the initial number of nodes.
    constructSession $username $sessionName $initNumUsrNode
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    xc2 cluster stop
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    # Start the cluster with the changed number of nodes
    xc2 -vv cluster start --num-nodes ${replayNumUsrNode}
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    cliCmd $username "session --list" || return 1

    cliCmd $username "session --switch --name $sessionName" | grep "$errorMsg"
    if [ $? -ne 0 ]; then
        echo "error msg does not match"
        return 1
    fi

    return 0

}

# test replay session on less number of usrnodes
replayOnLessTest()
{
    local initNumUsrNode=3
    local replayNumUsrNode=2

    local username="lessUsrNodeUsr"
    local sessionName="lessUsrNodeSession"

    replayOnDiffNumUsrNode $initNumUsrNode $replayNumUsrNode $username $sessionName
}

# test replay session on more number of usrnodes
replayOnMoreTest()
{
    local initNumUsrNode=2
    local replayNumUsrNode=3

    local username="moreUsrNodeUsr"
    local sessionName="moreUsrNodeSession"

    replayOnDiffNumUsrNode $initNumUsrNode $replayNumUsrNode $username $sessionName
}

#
# Check for replay failure after required UDF is copied elsewhere.  Then copy the UDF
# back and ensure replay succeeds.
#
missingUDFTest()
{
    local username="missingUDFUser"
    local sessionName="missingUDFSession"
    local ret=0
    local errorMsg="Error: server returned: Could not find function"

    constructSession $username $sessionName
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    xc2 cluster stop
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    # Save off the UDF file after emptying the "save" directory.
    mkdir -p "$persistedFilePath/udfs/python/replayTestSave"
    rm -rf "$persistedFilePath/udfs/python/replayTestSave/*"
    # Make sure the hard-coded absolute path name components below match those
    # defined in XLRDIR/src/include/log/Log.h (WorkBookDirName,
    # UdfWkBookDirName), and in XLRDIR/src/lib/libudf/LibUdfConstants.h
    # (sourcesPyName).
    mv "$persistedFilePath/workbooks/$username/$SessionId/udfs/python/"sessionReplayTest*.py "$persistedFilePath/udfs/python/replayTestSave/"
    if [ $? -ne 0 ]; then
        echo "Failed to move UDF file"
        return 1
    fi

    xc2 -vv cluster start --num-nodes ${XcalarQaNumNodes}
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    # As the UDF is missing we expect the session replay to fail
    cliCmd $username "session --list" || return 1
    cliCmd $username "session --switch --name $sessionName" | grep "$errorMsg"
    if [ $? -ne 0 ]; then
        echo "error msg does not match"
        return 1
    fi

    xc2 cluster stop
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    # Move the UDF file back to its original location
    # Make sure the hard-coded absolute path name components below match those
    # defined in XLRDIR/src/include/log/Log.h (WorkBookDirName,
    # UdfWkBookDirName), and in XLRDIR/src/lib/libudf/LibUdfConstants.h
    # (sourcesPyName).
    mv "$persistedFilePath/udfs/python/replayTestSave/"* "$persistedFilePath/workbooks/$username/$SessionId/udfs/python/"
    if [ $? -ne 0 ]; then
        echo "Failed to move back UDF file"
        return 1
    fi
    rmdir "$persistedFilePath/udfs/python/replayTestSave"

    xc2 -vv cluster start --num-nodes ${XcalarQaNumNodes}
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    # Replay should succeed as the UDF is back in the expected location
    cliCmd $username "session --list" || return 1
    cliCmd $username "session --switch --name $sessionName" || return 1

    cliCmd $username "inspect nonOpTable" | grep $ExpectedNumberOfRows
    if [ $? -ne 0 ]; then
        echo "Failed to replay session"
        return 1
    fi

    return 0
}

#
# Ensure replay fails after session file is corrupted.  Thee session file is
# corrupted by scribbling nonsense into the -meta file.
#
corruptedSessionTest()
{
    local username="corruptedSessionUser"
    local sessionName="corruptedSession"
    local ret=0
    local errorMsg="Incomplete session list (failed to read some sessions)"

    constructSession $username $sessionName
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    xc2 cluster stop
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    xc2 -vv cluster start --num-nodes ${XcalarQaNumNodes}
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    local sessionFilePath=`ls $persistedFilePath/sessions/Xcalar.session.corruptedSessionUser.*-meta`
    if [ $? -ne 0 ]; then
        echo "session file is missing"
        return 1
    fi

    # Corrupt the file by writing some nonsense into the meta file
    echo "corrupted session file" > $sessionFilePath

    cliCmd $username "session --list" | grep "$errorMsg"
    if [ $? -ne 0 ]; then
        echo "error msg does not match"
        return 1
    fi

    return 0
}

#
# Execute retina then stop/start the usrnode and ensure the replay succeeds.
# Then execute the retina a second time and check that the results are as
# expected.
#
retinaSessionReplayTest()
{
    local username="retinaSessionReplayUser"
    local sessionName="retinaSessionReplay"
    local ret=0

    xc2 -vv cluster start --num-nodes ${XcalarQaNumNodes}
    local ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    local retinaName="noopRetina"
    local retinaTableName="noopTable"
    local retinaFilePath="$XLRDIR/src/bin/tests/noopRetina.tar.gz"

    cliCmd $username "session --new --name $sessionName" || return 1
    cliCmd $username "session --switch --name $sessionName" || return 1
    cliCmd $username "retina load --retinaName $retinaName --retinaPath $retinaFilePath --verbose" || return 1
    cliCmd $username "executeRetina --retinaName $retinaName --dstTable $retinaTableName --parameters pathToQaDatasets:$pathToQaDatasets" || return 1

    cliCmd $username "inspect $retinaTableName" | grep $ExpectedNumberOfRows
    if [ $? -ne 0 ]; then
        echo "failed to replay session"
        return 1
    fi

    cliCmd $username "session --persist --name $sessionName" || return 1

    xc2 cluster stop
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    xc2 -vv cluster start --num-nodes ${XcalarQaNumNodes}
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    # the retina should have been restored on usrnode startup
    cliCmd $username "session --list" || return 1
    cliCmd $username "session --switch --name $sessionName" || return 1

    cliCmd $username "inspect $retinaTableName" | grep $ExpectedNumberOfRows
    if [ $? -ne 0 ]; then
        echo "fail to replay session"
        return 1
    fi

    return 0
}

#
# Ensure successful session replay with retina containing default module Udf (Xc-10325)
#
replayWithDefaultUdfTest()
{
    local username="defaultUdfReplayUser"
    local sessionName="defaultUdfSession"
    local ret=0

    xc2 -vv cluster start --num-nodes ${numUsrNode}
    local ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    local retinaName="defaultUdfRetina"
    local retinaFilePath="$XLRDIR/src/bin/tests/defaultUdfRetina.tar.gz"

    cliCmd $username "session --new --name $sessionName" || return 1
    cliCmd $username "retina load --retinaName $retinaName --retinaPath $retinaFilePath --verbose" || return 1

    cliCmd $username "session --persist --name $sessionName" || return 1

    xc2 cluster stop
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    xc2 -vv cluster start --num-nodes ${XcalarQaNumNodes}
    ret=$?
    if [ "$ret" != "0" ]; then
        return $ret
    fi

    cliCmd $username "session --list" || return 1
    cliCmd $username "session --switch --name $sessionName" || return 1

    # the retina should have been restored on usrnode startup
    cliCmd $username "executeRetina --retinaName $retinaName" || return 1

    cliCmd $username "retina delete $retinaName" || return 1

    return 0
}

addTestCase "gracefully shutdown test" gracefullyShutdown $TestCaseEnable ""
addTestCase "kill usrnode test" killedUsrnodeTest $TestCaseEnable ""
# disable DHT test - this is not really used and will not be in future
addTestCase "missing DHT test" missingDHTTest $TestCaseDisable ""
addTestCase "replay session on less number usrnode test" replayOnLessTest $TestCaseEnable ""
addTestCase "replay session on more number usrnode test" replayOnMoreTest $TestCaseEnable ""
addTestCase "missing UDF" missingUDFTest $TestCaseDisable ""
# XXX:
# replace corrupted session test with one that uses KVS to trigger corruption
# since session metadata now lives in KVS; disable until this is done
addTestCase "reading corrupted session" corruptedSessionTest $TestCaseDisable ""
addTestCase "session replay with retina export to table" retinaSessionReplayTest $TestCaseDisable "Will be added to pyTest"
addTestCase "replay retina containing default module UDF" replayWithDefaultUdfTest $TestCaseDisable "Will be added to pyTest"

runTestSuite

rm $YelpUserPathSL
rm $SessionReplayUDFPathSL

echo "1..$currentTestNumber"

xc2 cluster stop
tmpExitCode=$?
if [ "$tmpExitCode" != "0" ]; then
    echo "Failing sessionReplayTest because of unclean shutdown" 1>&2
    exit $tmpExitCode
fi
    

echo "*** sessionReplayTest.sh ENDING ON `/bin/date`"
