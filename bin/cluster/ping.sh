#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`

if [ $# -lt "0" -o $# -gt "0" ]; then
    echo "Usage: $0"
    exit 0
fi

ConfigFile=$HOME"/xcalar/src/data/cluster.cfg"
UserName="clusteruser"

. $DIR/setNodes.sh

for i in `seq 1 $NumNodes`;
do
    nodeId=$(($i - 1))

    ipAddr=${Nodes[$nodeId]}

    ping $ipAddr -c 1 -W 1 &> /dev/null
    pingable=$?

    if [ $pingable = "0" ]; then
        printf " -%-10s  up\n" "$ipAddr"
    else
        printf " -%-10s  down\n" "$ipAddr"
    fi
done
