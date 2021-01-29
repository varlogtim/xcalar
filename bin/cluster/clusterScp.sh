#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`

if [ $# -lt "2" -o $# -gt "2" ]; then
    echo "Usage: $0 localSrc clusterDest"
    exit 0
fi

ConfigFile=$HOME"/xcalar/src/data/cluster.cfg"
UserName="clusteruser"
Src=$1
Dst=$2

. $DIR/setNodes.sh

for i in `seq 1 $NumNodes`;
do
    nodeId=$(($i - 1))

    ipAddr=${Nodes[$nodeId]}

    echo "Executing on "$ipAddr
    scp $Src $UserName@$ipAddr:"$Dst" &
done
