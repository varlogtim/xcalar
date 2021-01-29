# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.



die () {
    echo >&2 "ERROR: $*"
    exit 2
}

preCheckFileName=$myHostName
preCheckFile="$TMP/$preCheckFileName"
mkdir -p $TMP || die "Fail to create directory $TMP"

touch $preCheckFile || die "Fail to create file $preCheckFile"

delimiter="="
hostname=`hostname`

unavailHosts=()

hostNameArray=(${hostNameList//,/ })

for Host in "${hostNameArray[@]}"
do
    ping -c 1 $Host >/dev/null 2>&1
    rc=$?
    if [[ $rc -ne 0 ]]
    then
        unavailHosts+=($Host)
    fi
done

numUnavailHost=${#unavailHosts[@]}
echo "${xcalarPreFix}Num_UnavailHost$delimiter$numUnavailHost" >> "$preCheckFile"

counter=1
for Host in "${unavailHosts[@]}"
do
    echo "${xcalarPreFix}UnavailHost_$counter$delimiter$Host" >> "$preCheckFile"
    counter=$((counter + 1))
done


echo "${xcalarPreFix}Hostname$delimiter$hostname" >> "$preCheckFile"

# IN KB
memTotal=`cat /proc/meminfo | grep MemTotal | awk -F' ' '{print $2}'`
echo "${xcalarPreFix}Memory$delimiter$memTotal" >> "$preCheckFile"

# IN KB
swapTotal=`cat /proc/meminfo | grep SwapTotal | awk -F' ' '{print $2}'`
echo "${xcalarPreFix}SwapTotal$delimiter$swapTotal" >> "$preCheckFile"

OSName=`cat /etc/os-release | grep "^NAME=" | awk -F'=' '{print $2}'`
echo "${xcalarPreFix}OS_NAME$delimiter$OSName" >> "$preCheckFile"

OSVersion=`cat /etc/os-release | grep "^VERSION=" | awk -F'=' '{print $2}'`
echo "${xcalarPreFix}OS_VERSION$delimiter$OSVersion" >> "$preCheckFile"

numSocket=`lscpu | grep "Socket(s)" | awk -F' ' '{print $2}'`
echo "${xcalarPreFix}NUM_SOCKET$delimiter$numSocket" >> "$preCheckFile"

numCorePerSocket=`lscpu | grep "Core(s) per socket:" | awk -F' ' '{print $4}'`
echo "${xcalarPreFix}NUM_CORE_PER_SOCKET$delimiter$numCorePerSocket" >> "$preCheckFile"

numThreadPerCore=`lscpu | grep "Thread(s) per core" | awk -F' ' '{print $4}'`
echo "${xcalarPreFix}NUM_THREAD_PER_CORE$delimiter$numThreadPerCore" >> "$preCheckFile"

NICArray=($(ifconfig  -s | awk -F' ' '{print$1}'))
NICArray=("${NICArray[@]:1}")
numNic=${#NICArray[@]}
echo "${xcalarPreFix}NUM_NIC$delimiter$numNic" >> "$preCheckFile"

counter=1
for NIC in "${NICArray[@]}"
do
    echo "${xcalarPreFix}NIC_$counter$delimiter$NIC" >> "$preCheckFile"
    netSpeed=`ethtool $NIC 2>/dev/null | grep Speed | awk -F' ' '{print $2}' `
    echo "${xcalarPreFix}$NIC$delimiter$netSpeed" >> "$preCheckFile"
    counter=$((counter + 1))
done

diskArray=($(lsblk -o NAME,SIZE,TYPE,ROTA | grep disk | awk -F' ' '{print$1}'))
numDisk=${#diskArray[@]}
echo "${xcalarPreFix}NUM_DISK$delimiter$numDisk" >> "$preCheckFile"

counter=1
for DISK in "${diskArray[@]}"
do
    echo "${xcalarPreFix}DISK_$counter$delimiter$DISK" >> "$preCheckFile"
    SIZE=`lsblk -o NAME,SIZE | grep -w "${DISK}" | awk -F' ' '{print$2}'`
    echo "${xcalarPreFix}${DISK}_SIZE$delimiter$SIZE" >> "$preCheckFile"

    SSD=`lsblk -o NAME,ROTA | grep -w "${DISK}" | awk -F' ' '{print$2}'`
    if [ $SSD = 0 ]
    then
        SSD="TRUE"
    else
        SSD="FALSE"
    fi
    echo "${xcalarPreFix}${DISK}_SSD$delimiter$SSD" >> "$preCheckFile"
    counter=$((counter + 1))
done
