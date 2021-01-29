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

# The command is in a file
cat <<EOF >> $TMP1
nodePid=\$(pgrep -u $UserName usrnode)
xcalardir=\`readlink -f /proc/\$nodePid/exe | grep -o .*xcalar\`
usrExecutable="\$xcalardir/src/bin/usrnode/usrnode"
echo "password" | sudo -S :
sudo /usr/bin/gdb -p \$nodePid \$usrExecutable
EOF

script=`cat $TMP1`

ssh -t $UserName@$ipAddr "$script"
rm -rf $TMP1
