#!/bin/bash
ipVal=$(($NODE_ID + 70))

ipAddr="192.168.100.$ipVal"

# Set up the device
#/sbin/ifconfig eth1 $ipAddr netmask 255.255.255.0
#/sbin/ifconfig eth1 mtu 9000

grep -q "eth0" /etc/network/interfaces
hasEth0=$?

if [ $hasEth0 = "1" ]; then
# Set up the table
cat << EOF >> /etc/network/interfaces

auto eth0
iface eth0 inet static
address $ipAddr
mtu 1500
netmask 255.255.255.0
broadcast 192.168.100.255
EOF
fi

ifdown eth0
ifdown eth1
ifup eth0
ifup eth1
