#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`

if [ $# -lt "0" -o $# -gt "0" ]; then
    echo "Usage: $0"
    exit 0
fi

ConfigFile="/netstore/users/dwillis/cfgs/cluster.cfg"
UserName="clusteruser"

. $DIR/setNodes.sh

echo

TMP1=`mktemp /tmp/clusterExec.XXXXX`
rm -rf $TMP1
mkdir $TMP1

for i in `seq 1 $NumNodes`;
do
    nodeId=$(($i - 1))

    ipAddr=${Nodes[$nodeId]}

    scriptFile="$TMP1/script$nodeId.sh"
    outputFile="$TMP1/output$nodeId.txt"
    touch $scriptFile
    touch $outputFile

    # The command is in a file
    cat <<EOF >> $scriptFile
    numUsrs=\$(pgrep -u \`whoami\` usrnode | wc -l)
    mgmtd=\$(pgrep -u \`whoami\` xcmgmtd | wc -l)
    name=\`hostname\`
    printf "%-10s %2d usrnodes   %d mgmtdaemon\n" "\$name" \$numUsrs \$mgmtd
EOF

    ssh $UserName@$ipAddr "bash" < $scriptFile > $outputFile &
done

wait

for i in `seq 1 $NumNodes`;
do
    nodeId=$(($i - 1))

    ipAddr=${Nodes[$nodeId]}

    outputFile="$TMP1/output$nodeId.txt"
    cat $outputFile
done

rm -rf $TMP1

echo
