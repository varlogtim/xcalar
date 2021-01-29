#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`

if [ $# -lt "2" -o $# -gt "2" ]; then
    echo "Usage: $0 <root?> <command>"
    exit 0
fi

ConfigFile=$HOME"/xcalar/src/data/cluster.cfg"
UserName="clusteruser"
Root=$1
Command=$2

if [ $Root = "root" ]
then
    echo "Executing as root..."
fi

. $DIR/setNodes.sh

TMP1=`mktemp /tmp/clusterExec.XXXXX`
rm -rf $TMP1
mkdir $TMP1
echo "Temp dir is $TMP1"

for i in `seq 1 $NumNodes`;
do
    nodeId=$(($i - 1))

    ipAddr=${Nodes[$nodeId]}

    scriptFile="$TMP1/script$nodeId.sh"
    outputFile="$TMP1/output$nodeId.txt"
    touch $scriptFile
    touch $outputFile

    # Set up the command file to pass the password
    if [ $Root = "root" ]; then
        echo "password" > $scriptFile
    fi

    echo "export NODE_ID='$nodeId'" >> $scriptFile

    # Add the actual script to the script file
    if [ -a "$Command" ]; then
        # The command is in a file
        cat "$Command" >> $scriptFile
    else
        echo "$Command" >> $scriptFile
    fi

    #echo "script is:"
    #cat $scriptFile

    if [ "$Root" = "root" ]; then
        ssh $UserName@$ipAddr "sudo -S bash" < $scriptFile > $outputFile &
    else
        ssh $UserName@$ipAddr "bash" < $scriptFile > $outputFile &
    fi
done

wait
echo

for i in `seq 1 $NumNodes`;
do
    nodeId=$(($i - 1))

    ipAddr=${Nodes[$nodeId]}

    echo "=*=*=*=*=*=*=*=*=*=*=*=*=*=$ipAddr=*=*=*=*=*=*=*=*=*=*=*=*=*="
    outputFile="$TMP1/output$nodeId.txt"
    cat $outputFile
done

rm -rf $TMP1


echo ""
echo ""
echo "Done cluster executing"
echo ""
