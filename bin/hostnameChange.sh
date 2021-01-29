#!/bin/bash

if [ $# -lt "1" ]
then
    echo "Usage: $0 <newHostname>"
    exit 0
fi

newHostname=$1
oldHostname=`hostname`

sudo sed -i 's/'$oldHostname'/'$newHostname'/g' /etc/hostname
sudo sed -i 's/'$oldHostname'/'$newHostname'/g' /etc/hosts
