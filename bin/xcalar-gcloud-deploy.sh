#!/bin/bash -e

function waitfornetwork {

until nc -zvw 1 $1 22
do
    sleep 2
done
}
function install_pkg {
#Check to see if gcloud is up or not?
[[ $(gcloud compute instances describe xcalar-cluster-0a --format yaml | grep status ) == *"status: TERMINATED"* ]] &&
    gcloud compute instances start xcalar-cluster-0a
XCALAR00_SERVER_ADDR=130.211.156.171
# We want to wait for the main node to startup and thus this loop that chescks for ssh
waitfornetwork $XCALAR00_SERVER_ADDR
# We are trying to find the name of the actual installer file
INSTALLER_FILE=$(basename $1)
#Copy the files over so we can start to  intall the application on the remote host
scp -o StrictHostKeyChecking=no   $1 $XCALAR00_SERVER_ADDR:~/.
# Using the  << we can stream comands into the ssh connection
ssh -o StrictHostKeyChecking=no   $XCALAR00_SERVER_ADDR <<EOF
chmod 755 ~/$INSTALLER_FILE
sudo bash -x ~/$INSTALLER_FILE
echo "Hello I am here Number 1! -Piccard"
EOF
}

# Create the snapshot:-
function snapshot {
    DATE=$(date +"%m%d%y%H%M")
    echo "Start Snapshot"
 gcloud compute disks snapshot xcalar-cluster-0a   --description "xcalar 2.3.0 installer disk $DATE by $USERID" --snapshot-names xcalar-snaphsot-$USERID-$DATE
}

#Create the Network
function network {
    gcloud compute --project "angular-expanse-99923" networks create "xcalar-cluster-network-$USERID" --range "10.240.0.0/16"
}

# Make copies of the snaphot and start them up
# We use waits to complete the disk creation from the snapshots first in parallel and then we start the VMs in parallel.
# It might be improved by combineing the loops but, the speed up is negligebale since we need all the machines online before we can do any work.

function spawnVMs {
for i in $(seq -f "%5g" 0 31)
do
    gcloud compute --project "angular-expanse-99923" disks create "xcalar-cluster-disk-$DATE-0${i}" --size "32" --zone "us-central1-f" --source-snapshot "xcalar-snaphsot-$USERID-$DATE" --type "pd-standard" &
done

wait

for i in $(seq -f "%5g" 0 31)
do
    gcloud compute --project "angular-expanse-99923" instances create "xcalar-test-cluster-$USERID-$DATE-0${i}" --zone "us-central1-f" --machine-type "n1-highmem-8" --network "default" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/cloud.useraccounts.readonly","https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/logging.write" --disk "name=xcalar-cluster-disk-$DATE-0${i}" "device-name=xcalar-cluster-0${i}" "mode=rw" "boot=yes" "auto-delete=yes" &

done

wait
}

# Create the template file and copy it over to the NFS location on the server
# We actully use the xclar-cluster-$DATE-0{$i} as the name since the local dns can resolve that, we may want to use IP addresses
# DNS names are a better abstraction for server end points than IP addresses but requires DNS to function relatively well.

function template_create {
#LIST_INSTANCE=`gcloud compute instances list | grep xcalar-cluster | grep -v xcalar-cluster-0a  | awk '{print $1}'`
#NUMBER_OF_NODES=32
echo "Node.NumNodes=32" >> template-2-$DATE.cfg
for i in $(seq 0 31)
do echo "
Node.$i.IpAddr=xcalar-test-cluster-$DATE-0${i}
Node.$i.Port=5000
Node.$i.ApiPort=18552" >> template-2-$DATE.cfg
done

#Combine the two files together to get the config
cat src/data/gclooud-default-template.cfg template-2-$DATE.cfg >> $DATE-xcalar-test-cluster.cfg
scp -o StrictHostKeyChecking=no  $DATE-xcalar-test-cluster.cfg $XCALAR00_SERVER_ADDR:/netstore/srv/shared/cluster/cfgs/$USERID-$DATE-xcalar-test-cluster.cfg
}

function xcalarstart {
    for ii in `gcloud compute instances list | grep xcalar-test-cluster-$USERID-$DATE | grep -v xcalar-cluster-0a | awk '{print $5}'`
    do
        waitfornetwork $ii
        ssh-keygen -R $ii
        ssh $ii  -o StrictHostKeyChecking=no sudo service xcalar start &
    done
    wait
}


function cleanyourroom {
   for ii in  $( gcloud compute instances list | grep -v xcalar-cluster-0a| grep -v NAME| grep xcalar-test-cluster-$USERID | awk '{print $1}')
    do
        yes y |  gcloud compute instances delete $ii --delete-disks all  --zone "us-central1-f" &
    done
}

if [ -z "$1" ]
  then
    echo "No argument supplied\n
 Either supply the installer file e.g. xcalar-gcloud-deploy.sh  xcalar-installer.tar \n
 Or xcalar-gcloud-deploy.sh cleanup to clean up all the test VMs"
exit 1
fi

if  [ $1 = "cleanup" ]; then
    cleanyourroom
else
    echo "Starting install"
install_pkg $1
echo "Starting snapshot"
snapshot
echo "Starting spawning VMS"
spawnVMs
echo "Restart Xcalar Service"
xcalarstart
echo "Xcalar has been started please check the machines at gcloud compute instances list"
fi
