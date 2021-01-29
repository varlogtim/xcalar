#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`

userName="clusteruser"
password="password"

. $DIR/setNodes.sh

echo "Checking for sshpass"
dpkg-query -l sshpass > /dev/null
ret=$?
if [ "ret" = "1" ]; then
    echo "Need to install sshpass"
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y sshpass
else
    echo "sshpass is installed"
fi

for nodeId in `seq 0 $((NumNodes - 1))`;
do
    ipAddr=${Nodes[$nodeId]}

    sshpass -p "$password" \
        ssh $userName@$ipAddr "cat ~/.ssh/authorized_keys" | \
        grep -q "`whoami`@`hostname`"
    ret=$?

    if [ "$ret" = "0" ]; then
        echo "$ipAddr is already set up"
    else
        echo "Setting up $ipAddr for password-less ssh"
        cat ~/.ssh/id_rsa.pub | sshpass -p "$password" \
            ssh $userName@$ipAddr 'cat >> ~/.ssh/authorized_keys'
    fi

    echo "Adding $ipAddr to known hosts"
    ssh-keyscan -H $ipAddr >> ~/.ssh/known_hosts
done

echo "Testing passwordless ssh"

for nodeId in `seq 0 $((NumNodes - 1))`;
do
    ipAddr=${Nodes[$nodeId]}

    ssh $userName@$ipAddr "cat /etc/hostname" | grep -q $ipAddr
    ret=$?

    if [ "$ret" != "0" ]; then
        echo "$ipAddr failed to set up"
    else
        echo "$ipAddr set up correctly"
    fi
done

