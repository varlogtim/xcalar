#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`

declare -a Nodes

ConfigFile="/netstore/users/dwillis/cfgs/cluster.cfg"
UserName="clusteruser"

if [ "$ConfigFile" == "" ]; then
    echo "Need to specify the ConfigFile variable"
    exit 1
fi

if [ ! -f "$ConfigFile" ]; then
    echo "Need ConfigFile specified; \"$ConfigFile\" doesn't exist"
    exit 1
fi

NumNodes=`cat $ConfigFile | grep NumNodes | cut -d = -f 2`

while read line
do
    Key=`echo $line | cut -d = -f 1`
    Module=`echo $Key | cut -d . -f 1`
    if [ "$Module" = "Node" ]; then
        nodeId=`echo $Key | cut -d . -f 2`
        InnerKey=`echo $Key | cut -d . -f 3`
        if [ "$InnerKey" = "IpAddr" ]; then
            ipAddr=`echo $line | cut -d = -f 2`
            Nodes[${nodeId}]=$ipAddr
        fi
        if [ "$InnerKey" = "ApiPort" ]; then
            portVal=`echo $line | cut -d = -f 2`
            Ports[${nodeId}]=$portVal
        fi
    fi
done < $ConfigFile

duplicatedHosts=`cat $ConfigFile | grep IpAddr | cut -d = -f 2 | sort | uniq -d`

numDuplicates=`cat $ConfigFile | grep IpAddr | cut -d = -f 2 | sort | uniq -d | wc -l`
if [ $numDuplicates != "0" ]; then
    echo "The following hosts appear more than once in the config file:"
    echo "$duplicatedHosts"
    echo "Please remove all duplicates"
    exit 1
fi

