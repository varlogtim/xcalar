#!/bin/bash

# Copyright 2015 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

function usage
{
    echo "usage: clusterInstall.sh -h <nfs_host> -c <nfs_cfg_path> -i <installer_path>" >&2
    echo "                         [-u <username>] [-p <password>] [-e <cli_path>] [-a]" >&2
    echo "                         <node0> [<node1> ... <nodeN>]" >&2
    echo "" >&2
    echo " -h <nfs_host> Hostname of NFS share." >&2
    echo " -c <nfs_cfg_path> Path within NFS share to write Xcalar config files." >&2
    echo " -i <installer_path> Xcalar installer." >&2
    echo " -u <username> -p <password> Username/password required to authenticate to nodes." >&2
    echo " -e <cli_path> Path to xccli. Used to verify installation." >&2
    echo " -a Perform installs in parallel in the background (no input possible)." >&2
    echo " <node0> [<node1> ... <nodeN>] Hostnames of nodes on which Xcalar will be" >&2
    echo "                               installed." >&2
}

function fail
{
    # Print error message and/or usage then exit.

    local message=$1
    local skipUsage=$2

    echo ""

    if [ "$message" != "" ]; then
        # Print $message in red.
        echo -e "\e[31m${message}\e[0m" >&2
        echo "" >&2
    fi

    if [ "$skipUsage" = false -o "$skipUsage" = "" ]; then
        usage
        echo ""
    fi

    exit 1
}

function install
{
    local node=$1

    commands="export DEBIAN_FRONTEND=noninteractive && \
              apt-get update && \
              apt-get -y install nfs-common && \
              /var/tmp/$installerName && \
              mount ${nfsHost}:${nfsCfgPath} /etc/xcalar && \
              echo '${nfsHost}:${nfsCfgPath} /etc/xcalar nfs defaults 0 0' >> /etc/fstab && \
              service xcalar restart "

    if [ -z "$password" ]; then
        # Copy installer onto node.
        scp -o StrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null "$installerPath" \
            "${username}@${node}:/var/tmp/" &> /dev/null

        if [ $parallel != false ]; then
            # Executing in parallel. stdout needs to go in a file.
            ssh -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null "${username}@${node}" \
                "sudo su -c \"$commands\"" > "$wrkDir/installOut/$node"
        else
            # Executing sequentially. Force allocate tty so that user can give needed passwords.
            ssh -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -t "${username}@${node}" \
                "sudo su -c \"$commands\""
        fi
    else
        # Use helper expect scripts to pass password in where it's needed.
        scriptDir=`dirname ${BASH_SOURCE[0]}`
        sshpass -p "$password" scp \
            -oStrictHostKeyChecking=no \
            -oUserKnownHostsFile=/dev/null \
            "$installerPath" "${username}@${node}:/var/tmp/" &> /dev/null
        echo -e "$password\n$commands" |
            sshpass -p "$password" ssh \
            -oStrictHostKeyChecking=no \
            -oUserKnownHostsFile=/dev/null \
            "$username@$node" "sudo -S bash" \
            &> "$wrkDir/installOut/$node"
    fi

    echo "[Node ${node}] Installer completed. Additional output: '$wrkDir/installOut/$node'."
}


#
# Parse and validate arguments.
#

parallel=false
while getopts ":h:ac:i:u:p:g:e:" opt; do
    case $opt in
        h)
            nfsHost="$OPTARG"
            ;;
        c)
            nfsCfgPath="$OPTARG"
            ;;
        i)
            installerPath="$OPTARG"
            ;;
        u)
            username="$OPTARG"
            ;;
        p)
            password="$OPTARG"
            ;;
        a)
            parallel=true
            ;;
        e)
            cliPath="$OPTARG"
            ;;
        :)
            fail "-${OPTARG} requires an argument."
            ;;
        \?)
            fail "Invalid option -${OPTARG}."
            ;;
    esac
done

# Remaining args are nodes.
shift $(( $OPTIND - 1 ))
nodes="$@"

if [ -z "$nfsHost" ]; then
    fail "-h (NFS host name) required."
fi
if [ -z "$nfsCfgPath" ]; then
    fail "-c (NFS config path) required."
fi
if [ -z "$installerPath" ]; then
    fail "-i (installer path) required."
fi
if [ ! -f "$installerPath" ]; then
    fail "Installer '$installerPath' not found."
fi
if [ -z "$nodes" ]; then
    fail "Must specify at least one node."
fi

if [ -z "$password" ] && [ $parallel != false ]; then
    echo "Warning: installers will be executed in parallel which prevents passwords" >&2
    echo "from being entered via stdin. Unless SSH authentication is configured, it" >&2
    echo "may be necessary to pass passwords in via -p or use the -s flag to execute" >&2
    echo "sequentially." &>2
fi

#
# Generate config file and write to given NFS path.
#
wrkDir=/tmp/xcalarInstall
cfgDir="$wrkDir/nfsCfg"
sudo rm -rf $wrkDir
sudo mkdir $wrkDir
sudo mkdir $cfgDir
sudo chmod --recursive 0777 $wrkDir
sudo mount ${nfsHost}:${nfsCfgPath} $cfgDir
if [ "$?" != "0" ]; then
    fail "Failed to mount NFS share."
fi
configFile="$cfgDir/default.cfg"

sudo tee $cfgDir/template.cfg > /dev/null <<EOF
// Run cluster with the below parameters

Constants.XcalarRootCompletePath=/var/opt/xcalar

Thrift.Port=9090
Thrift.Host=localhost

// Cluster management stuff. The following has been
// auto-generated by genConfig.sh
EOF
cp "$cfgDir/template.cfg" $configFile
echo "Node.NumNodes=$(echo $nodes | wc -w)" >> $configFile

i=0
for node in $nodes; do
    echo "Node.${i}.IpAddr=${node}" >> $configFile
    echo "Node.${i}.Port=$(( 5000 + $i ))" >> $configFile
    echo "Node.${i}.ApiPort=18552" >> $configFile
    echo "Node.${i}.MonitorPort=$(( 8000 + $i ))" >> $configFile
    i=$(( $i + 1 ))
done

sudo umount $cfgDir
sudo rmdir $cfgDir


#
# Connect to each node and run installer.
#

installerName=`basename $installerPath`
cat > "$wrkDir/commands" <<EOF
$password
apt-get -y install nfs-common
~/$installerName
mount ${nfsHost}:${nfsCfgPath} /etc/xcalar
echo "${nfsHost}:${nfsCfgPath} /etc/xcalar nfs defaults 0 0" >> /etc/fstab
service xcalar restart
EOF

if [ -z "$username" ]; then
    username="$USER"
fi

mkdir "$wrkDir/installOut"

for node in $nodes; do
    echo "Installing on $node"
    if [ $parallel != false ]; then
        install "$node" &
    else
        # Must not install in parallel if we're relying on input.
        install "$node"
    fi
    echo "Finished installing on $node"
done
wait


#
# Run a quick check to verify the installation was successful and each node can
# communicate with the other nodes.
#

if [ ! -z "$cliPath" ]; then
    nodeCount=`echo $nodes | wc -w`

    for node in $nodes; do
        $cliPath -c "connect $node; top" &> "$wrkDir/installOut/$node-status"
        lookingFor=0
        while read line; do
            case "$line" in
                Connected*)
                    ;;
                Node*)
                    ;;
                \=\=*)
                    ;;
                $lookingFor*)
                    lookingFor=$(( $lookingFor + 1 ))
                    ;;
                Error*)
                    echo "[Node ${node}] Error connecting to node:"
                    echo "[Node ${node}] $line"
                    break
                    ;;
            esac
        done < "$wrkDir/installOut/$node-status"
        if [ $lookingFor -eq $nodeCount ]; then
            echo "[Node ${node}] Node is running and has connected to all other nodes."
        fi
    done
fi
