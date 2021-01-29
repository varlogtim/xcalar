#!/bin/bash

declare -a devNames=("/dev/sdb" "/dev/sdc")
swapDevName="/dev/sdd"

# Iterate through all device names
for ii in `seq 1 ${#devNames[@]}`
do
    deviceName=${devNames[$((ii - 1))]}
    mountName="/storage"$((ii))
    echo -e "o\nn\np\n1\n\n\nw" | sudo fdisk $deviceName

    partitionName="$deviceName"1

    # Make the file system
    sudo mkfs.ext4 $partitionName

    # Mount the new partition
    sudo mkdir $mountName
    sudo mount $partitionName $mountName

    # Setup user level storage
    sudo mkdir "$mountName/data"
    sudo chmod +777 "$mountName/data"

    # Set up this disk in fstab
    thisUuid=`blkid $partitionName | cut -d \" -f 2`

    sudo su -c "echo 'UUID=$thisUuid $mountName ext4 defaults 0 0' >> /etc/fstab"
done

# Set up swap
echo -e "o\nn\np\n1\n\n\nw" | sudo fdisk $swapDevName
swapPartitionName="$swapDevName"1

# Remove the old swap
sudo sed -i "\/dev\/mapper\/bach--vg-swap/d" /etc/fstab

# Wait for a while before adding the new swap,
# to allow the blkid daemon to find it
sleep 20
swapUuid=`sudo blkid $swapPartitionName | cut -d \" -f 2`
sudo mkswap "$swapPartitionName"
sudo swapon "$swapPartitionName"

# Setup fstab
sudo su -c "echo 'UUID=$swapUuid none swap sw 0 0' >> /etc/fstab"

# Remove the old swap
sudo sed -i "\/dev\/mapper\/bach--vg-swap/d" /etc/fstab
