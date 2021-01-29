#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`
XLRROOT=$DIR/../../..
pathToNodesLib=$XLRROOT/bin
pathToUsrNode=$XLRROOT/src/bin/usrnode

TERMINAL=${TERMINAL:-lxterminal}
Profile=${1:-silent}

configFile=$DIR/temp.cfg

TMP1=`mktemp /tmp/launcher.XXXXX`
rm -rf $TMP1

mkdir $TMP1
echo "Creating launcher output in $TMP1"

while [ 1 ];
do
    cmd=`nc -l 31337`
    if [ "$cmd" = "SAVE" ]; then
        nc -l 31337 > $configFile
        echo "Saved config at $configFile"
    elif [ "$cmd" = "ORIGIP" ]; then
        OrigIp=`nc -l 31337`
        echo "Got master IP $OrigIp"
    elif [ "$cmd" = "PROFILE" ]; then
        Profile=`nc -l 31337`
        echo "Got profile $Profile"
    elif [ "$cmd" = "NUMNODES" ]; then
        NumNodes=`nc -l 31337`
        echo "Got NumNodes $NumNodes"
    elif [ "$cmd" = "LAUNCH" ]; then
        nodeId=`nc -l 31337`
        echo "Launching $args"
        if [ "$Profile" = "silent" ]; then
            . $pathToNodesLib/nodes.sh
            spawnOneNode "$nodeId" "$NumNodes" "$TMP1"
        else
            if [ "$TERMINAL" = "gnome-terminal" ]; then
                $TERMINAL --tab --profile "$Profile" -e "$DIR/usrnode --configFile $configFile --nodeId $nodeId --numNodes $NumNodes" &
            else
                $TERMINAL -e "$DIR/usrnode --configFile $DIR/temp.cfg --nodeId $nodeId --numNodes $NumNodes" &
            fi
        fi

        echo "Acking back to $OrigIp"
        sleep 1
        echo "ACK" | nc $OrigIp 31338
    fi
done;
