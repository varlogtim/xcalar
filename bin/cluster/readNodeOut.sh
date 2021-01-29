#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`

if [ $# -lt "1" -o $# -gt "1" ]; then
    echo "Usage: $0 <nodeNum>"
    exit 0
fi


ConfigFile=$HOME"/xcalar/src/data/cluster.cfg"
UserName="clusteruser"

. $DIR/setNodes.sh
nodeId=$1

ipAddr=${Nodes[$nodeId]}

TMP1=`mktemp /tmp/clusterExec.XXXXX`
rm -rf $TMP1
touch $TMP1

TMP2=`mktemp /tmp/clusterExec.XXXXX`
rm -rf $TMP2
touch $TMP2

# The command is in a file
cat <<EOF >> $TMP1
nodeDir=/tmp/\$(ls -t /tmp | grep launcher | head -n1 | cut -d : -f 1)
nodeFile="\$nodeDir/node.$nodeId.out"
cat \$nodeFile
EOF

ssh $UserName@$ipAddr "bash" < $TMP1 > $TMP2
rm $TMP1

$EDITOR $TMP2
rm $TMP2
