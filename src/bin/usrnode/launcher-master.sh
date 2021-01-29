#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`

if [ $# -lt "1" -o $# -gt "3" ]; then
    echo "Usage: $0 <numNodes> silent <configFile>"
    exit 0
fi

NumNodes=$1
Profile=${2:-silent}
ConfigFile=$3

declare -A ipAddresses

if [ "$ConfigFile" = "" ]; then
    ConfigFile=$DIR/test-config.cfg
fi

while read line
do
    Key=`echo $line | cut -d = -f 1`
    Module=`echo $Key | cut -d . -f 1`
    if [ "$Module" = "Node" ]; then
        nodeId=`echo $Key | cut -d . -f 2`
        InnerKey=`echo $Key | cut -d . -f 3`
        if [ "$InnerKey" = "IpAddr" ]; then
            ipAddr=`echo $line | cut -d = -f 2`
            ipAddresses[$ipAddr]="0"
            nodes[${nodeId}]=$ipAddr
        fi
    fi
done < $ConfigFile

myIpAddr=`ifconfig | grep "inet addr" -m 1 | cut -d : -f 2 | cut -d \  -f 1`
myHostname=`hostname`

for i in `seq 1 $NumNodes`;
do
    nodeId=$(($i - 1))

    ipAddr=${nodes[$nodeId]}

    if [ "${ipAddresses[$ipAddr]}" = "0" ]; then
        ipAddresses[$ipAddr]="1"
        echo "ORIGIP" | nc $ipAddr 31337
        sleep 1

        echo "$myIpAddr" | nc $ipAddr 31337
        sleep 1

        echo "SAVE" | nc $ipAddr 31337
        sleep 1

        cat $ConfigFile | nc $ipAddr 31337
        sleep 1

        echo "PROFILE" | nc $ipAddr 31337
        sleep 1

        echo "$Profile" | nc $ipAddr 31337
        sleep 1

        echo "NUMNODES" | nc $ipAddr 31337
        sleep 1

        echo "$NumNodes" | nc $ipAddr 31337
        sleep 1
    fi

    echo "Launching node $nodeId @ $ipAddr"
    echo "LAUNCH" | nc $ipAddr 31337
    sleep 1

    echo "$nodeId" | nc $ipAddr 31337

    ack=`nc -l 31338`
    sleep 1

#    gnome-terminal --tab --profile $Profile -e "$DIR/usrnode $UseFlash --nodeId $nodeId --numNodes $NumNodes --configFile $DIR/test-config.cfg"
done
