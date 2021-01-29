#!/bin/bash

# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.



masterHost=`hostname`
keyFileSet=false
xcalarPrefix="XCALAR_"
maxWaitingTime=10

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TMP="${TMPDIR:-/tmp}/$LOGNAME/$(basename ${BASH_SOURCE[0]})/$$"
mkdir -p $TMP || die "Fail to create directory $TMP"

# clean up on exit
trap "rm -rf $TMP" EXIT

preMem=0
prevHost=""
# 64GB in MB
minMem=65536

kbPerMB=1024
minNicSpeedInMb=10000
minNicSpeedInGb=10

die () {
    echo >&2 "ERROR: $*"
    exit 2
}

warning () {
    # is stderr a terminal?

    if [ -t 2 ]; then
        echo -e >&2 "\e[31mWARNING: $@\e[0m"
    else
        echo >&2 "WARNING: $@"
    fi
}

usage()
{
    echo "Usage:" 1>&2
    echo "        PreInstallationChecker.sh -h <hostnames> -u <username [-p <password>] [-i <privateKeyFile>] [-d <HdfsUrl>] [-n <NfsName>] [-m <NfsMountPoint>]" 1>&2
    echo "" 1>&2
    echo "        Must specify either username/password or private key file" 1>&2
    echo "        Must specify either HDFS URL or NFS server name and mount point" 1>&2
    echo "        Hostname must be seperated by comma" 1>&2
    exit 1
}

checkHost()
{
    . $1

    eval Hostname='$'"${xcalarPrefix}Hostname"
    echo -e "Checking Host:$Hostname"

    if [ -z "$Hostname" ]; then
        warning "Fail to get hostname"
    fi

    eval numUnAvailHost='$'"${xcalarPrefix}Num_UnavailHost"
    if [ $numUnAvailHost -gt 0 ]; then
        for ii in $(seq 1 $numUnAvailHost);
        do
            eval unavailHost='$'"${xcalarPrefix}UnavailHost_$ii"
            warning "$unavailHost is not reachable"
        done
    fi

    eval Memory='$'"${xcalarPrefix}Memory"

    if [ -z "$Memory" ]; then
        warning "Fail to get memory size"
    else
        Memory=$((Memory/kbPerMB))
        echo "Memory: $Memory MB"
        if [ $preMem -eq 0 ]; then
            preMem=$Memory
            prevHost=$Host
        else
            if [ $preMem -ne $Memory ]; then
                warning "$Host memory size is different than $prevHost   "
            fi
            preMem=$Memory
            prevHost=$Host
        fi

        if [ $Memory -lt $minMem ]; then
            warning "$Host has less memory than recommended. Recommend minimum is $minMem MB"
        fi
    fi

    eval Swap='$'"${xcalarPrefix}SwapTotal"

    Swap=$((Swap/kbPerMB))
    echo "Swap: $Swap MB"

    eval OSName='$'"${xcalarPrefix}OS_NAME"
    echo "OS Name: $OSName"

    eval OSVersion='$'"${xcalarPrefix}OS_VERSION"
    echo "OS Version: $OSVersion"

    eval numSocket='$'"${xcalarPrefix}NUM_SOCKET"
    echo "Number of Sockets: $numSocket"

    eval numCorePerSocket='$'"${xcalarPrefix}NUM_CORE_PER_SOCKET"
    echo "Number of Cores Per Socket: $numCorePerSocket"

    eval numThreadPerCore='$'"${xcalarPrefix}NUM_THREAD_PER_CORE"
    echo "Number of Thread Per Core: $numThreadPerCore"

    eval numNic='$'"${xcalarPrefix}NUM_NIC"

    No10GBNIC=true
    for ii in $(seq 1 $numNic);
    do
        eval nicName='$'"${xcalarPrefix}NIC_$ii"
        eval NicSpeed='$'"${xcalarPrefix}$nicName"
        if [ -z "$NicSpeed" ]; then
            continue
        fi
        echo "Network Interface ${nicName}'s speed: $NicSpeed"

        echo "$NicSpeed" | grep -q "Mb"

        if [ $? -eq 0 ]; then

            NicSpeedNum=`echo "$NicSpeed" | grep -o '[0-9]\+'`

            if [ $NicSpeedNum -gt $kbPerMB ]; then
                No10GBNIC=false
            fi
        fi

        echo "$NicSpeed" | grep -q "Gb"

        if [ $? -eq 0 ]; then

            NicSpeedNum=`echo "$NicSpeed" | grep -o '[0-9]\+'`

            if [ $NicSpeedNum -gt $minNicSpeedInGb ]; then
                No10GBNIC=false
            fi
        fi

    done

    if [ $No10GBNIC = "true" ]; then
        warning "NO 10Gb NIC avaiable"
    fi

    eval NumDisk='$'"${xcalarPrefix}NUM_DISK"

    for ii in $(seq 1 $NumDisk);
    do
        eval DiskName='$'"${xcalarPrefix}DISK_$ii"
        eval DiskSize='$'"${xcalarPrefix}${DiskName}_SIZE"
        eval IsSSD='$'"${xcalarPrefix}${DiskName}_SSD"
        if [ "$IsSSD" = TRUE ]; then
            echo "Storage Device ${DiskName}'s size: $DiskSize, type: SSD"
        else
            echo "Storage Device ${DiskName}'s size: $DiskSize, type: HDD"
        fi

    done

}

while getopts "h:u:p:i:d:n:m:" opt; do
  case $opt in
      h) hostNameList=$OPTARG;;
      u) userName=$OPTARG;;
      p) password=$OPTARG;;
      i) keyFilePath=$OPTARG;;
      d) HDFSURL=$OPTARG;;
      n) NFSName=$OPTARG;;
      m) NFSMountPoint=$OPTARG;;
      *) usage;;
  esac
done

if [ "$hostNameList" = "" ]
then
    echo "-h <hostname> is a required argument" 1>&2
    usage
fi

if [ "$userName" = "" ]
then
    echo "-u <username> is a required argument" 1>&2
    usage
fi

if [ "$password" = "" ]
then
    if [ "$keyFilePath" = "" ]
    then
        echo "Must specify -p <password> or -i <privateKeyFile> " 1>&2
        usage
    fi
    keyFileSet=true
fi

if [ "$NFSName" = "" ] || [ "$NFSMountPoint" = "" ]
then
    if [ "$HDFSURL" = "" ]
    then
        echo "Must specify -n <NfsName> -m <NfsMountPoint> or -d <HdfsUrl> " 1>&2
        usage
    fi
fi

availHosts=()

hostNameArray=(${hostNameList//,/ })
for Host in "${hostNameArray[@]}"
do
    ping -c 1 $Host >/dev/null 2>&1
    rc=$?
    if [[ $rc -ne 0 ]]
    then
        warning "$Host is not reachable"
    else
        availHosts+=($Host)
    fi
done

NUMHOST=${#hostNameArray[@]}

if [ "$keyFileSet" = true ]
then
    for Host in "${availHosts[@]}"
    do
        printf "#!/bin/bash\nmyHostName=$Host\nhostNameList=$hostNameList\nuserName=$userName \npassword=$password\nkeyFilePath=$keyFilePath\nxcalarPreFix=\"XCALAR_\"\nTMP=$TMP\n" | cat - "$DIR/PreInstallationCheckerLocal.sh" > $TMP/temp

        ssh -i $keyFilePath $userName@$Host 'bash ' < $TMP/temp
    done


    remainingHosts=("${availHosts[@]}")
    numRemainingHost=${#remainingHosts[@]}
    counter=0

    while : ; do
        hostNotReady=()
        if [ "$numRemainingHost" -eq 0 ]; then
            break
        fi

        for Host in "${remainingHosts[@]}"
        do
            scp -i $keyFilePath $userName@$Host:$TMP/$Host $TMP/ > /dev/null 2>&1

            if [ $? -ne 0 ]; then
                hostNotReady+=($Host)
            else
                checkHost $TMP/$Host
                ssh -i $keyFilePath $userName@$Host "rm -rf $TMP/$Host"
            fi
        done

        remainingHosts=("${hostNotReady[@]}")
        numRemainingHost=${#hostNotReady[@]}

        counter=$((counter + 1))
        if [ $counter -gt $maxWaitingTime ]; then

            for Host in "${hostNotReady[@]}"
            do
                warning  "$Host is not responding"
            done

            exit 1
        fi
        sleep 1

    done
fi

exit 0
