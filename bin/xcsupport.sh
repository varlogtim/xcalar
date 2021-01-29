#!/bin/bash

#
# Xcalar internal use only
#
# Xcalar Support facility
#
# list and download customer Support data


export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-west-2}

XCASUPTMP="/var/tmp/xcasup"
mkdir -p "$XCASUPTMP"
XCASUPCACHE="$XCASUPTMP/xcasup.uploads.cache"
UPDATECACHECMD="aws s3 ls --recursive s3://xcasup/uploads/"

# amit says ls is expensive on AWS and should be cached
if [ ! -e $XCASUPCACHE ]
then
    $UPDATECACHECMD > $XCASUPCACHE
    if [ "$?" != "0" ]
    then
        echo "Please setup ASUP access on your machine (http://wiki.int.xcalar.com/mediawiki/index.php/ASUPs)" 1>&2
        exit 1
    fi
fi

usage()
{
    echo "Usage:" 1>&2
    echo "        xcsupport.sh -f <asupId> [-t <timestamp>] [-o <outdir>] [-u <hostname>] # fetch ASUP; default timestamp is latest" 1>&2
    echo "        xcsupport.sh -f <full-path-to-asup> [-t <timestamp>] [-o <outdir>] [-u <hostname>] # fetch ASUP; default timestamp is latest" 1>&2
    echo "        xcsupport.sh -l [-a <asupid>] [-u <hostname>] # list remote support bundles (cached)" 1>&2
    echo "        xcsupport.sh -r # refresh cache of remote support bundles" 1>&2
    echo "        xcsupport.sh -h" 1>&2
    exit 1
}

aws_check_access()
{
    if ! aws s3 cp s3://xcasup/uploads/test/test - >/dev/null; then
        echo >&2 "Your AWS isn't configured correctly. Please see https://xcalar.atlassian.net/wiki/spaces/EN/pages/8749395/Vault#Vault-AWSAccess"
        exit 1
    fi
}

CMD=""
TARGETHOST=""
TIMESTAMP=""
ASUPID=""
XCASUPDIR="$XCASUPTMP"

while getopts "f:t:o:u:a:rlh" opt; do
  case $opt in
      f) CMD="fetch" && ASUPID=$OPTARG;;
      a) ASUPID="$OPTARG";;
      o) XCASUPDIR="$OPTARG";;
      t) TIMESTAMP=$OPTARG;;
      l) CMD="ls";;
      u) TARGETHOST=$OPTARG;;
      r) CMD="refresh";;
      *) usage;;
  esac
done

if [ "$CMD" = "" ]
then
    usage
fi

mkdir -p "$XCASUPDIR"

TMPD=`mktemp --tmpdir -d xcsupport.XXXXXX`
trap "rm -rf $TMPD" EXIT

if [ "$CMD" = "refresh" ]
then
    set -x
    $UPDATECACHECMD > $XCASUPCACHE
    ret=$?
    set +x
    exit $ret
fi

 listSubs()
 {
    local subIdToSearch="${1:-.}"
    grep "$subIdToSearch" "$XCASUPCACHE" | grep "${TIMESTAMP:-.}" | grep "${TARGETHOST:-.}" | while read ln
    do
        subDate=`echo $ln | cut -f1,2 -d' '`
        subId=`echo $ln | sed 's/.*supportPack//' | sed 's/\.tar\.gz//'`
        subHost=`echo $ln | sed 's/.*uploads\/[0-9][0-9]*//' | cut -f2 -d/`
        subCase=`echo $ln | awk -F "/" '$4 !~ /supportPack/{print $4}'`
        echo $subDate $subId $subHost $subCase
    done
}

if [ "$CMD" = "ls" ]
then
    (
    echo "DATE TIME SUBID HOST CASEID"
    listSubs $ASUPID
    ) | column -t
    exit 0
fi

if [ "$CMD" != "fetch" ]
then
    echo "Unknown CMD: $CMD" 1>&2
    exit 1
fi

if [ "$ASUPID" = "" ]
then
    usage
fi

if aws s3 ls "s3://xcasup/uploads/$ASUPID" >/dev/null 2>&1; then
    unixTime="${ASUPID%%/*}"
    TIMESTAMP="$(date -d@$unixTime +'%Y-%m-%d %H:%M:%S')"
    fileToFetch="uploads/$ASUPID"
else
    listSubs $ASUPID > $TMPD/rsubsFound
    foundCount=`wc -l $TMPD/rsubsFound | cut -f1 -d' '`
    if [ "$TIMESTAMP" = "" ]
    then
        TIMESTAMP=`cat $TMPD/rsubsFound | sort | tail -1 | cut -f1,2 -d' '`
        echo "Found $foundCount SUBs; defaulting to latest ($TIMESTAMP)" 1>&2
    fi
    fileToFetch=`cat $XCASUPCACHE | grep -v '/tesla/thrift' | grep $ASUPID | grep "$TIMESTAMP" | sed 's/.*uploads/uploads/'`
    if [ "$fileToFetch" = "" ]
    then
        echo "Failed to find SUB for SUBID $ASUPID TIMESTAMP $TIMESTAMP" 1>&2
        exit 1
    fi
fi
timeStampNoSpc=`echo $TIMESTAMP | sed 's/ /_/g' | sed 's/:/_/g' | sed 's/\-/_/g'`
localFile="`echo $fileToFetch | sed 's@.*/@@' | sed 's/supportPack//'`.${timeStampNoSpc}"
extractDir="`echo $fileToFetch | sed 's@.*/@@' | sed 's/\.tar\.gz//' | sed 's/supportPack//'`.${timeStampNoSpc}"

curdir=`pwd`
cd "$XCASUPDIR"
if [ ! -e $localFile ] && [ ! -d $extractDir ]
then
    aws_check_access
    aws s3 cp s3://xcasup/$fileToFetch $localFile
    # wget https://xcasup.s3.amazonaws.com/$fileToFetch -O $localFile
    if [ "$?" != "0" ]
    then
        echo "Failed to download $fileToFetch" 1>&2
        exit 1
    fi
fi

if [ ! -d $extractDir ]
then
    mkdir -p $extractDir
    cd $extractDir
    tar -zxvf ../$localFile
    mv supportData/* .
    rmdir supportData
    rm -f ../$localFile
    cd ..
fi

cd $extractDir
# unzip
for gzFile in `find . -name \*.gz`
do
    echo "gunzip $gzFile..."
    gunzip $gzFile
done

# convert .crash files to core files
for crashFile in `find . -name \*.crash`
do
    dstFile=`echo $crashFile | sed 's/.crash$//' | sed 's/_opt_xcalar_bin_/core\./'`

    rescueCoreDump.sh $crashFile $dstFile
done

echo "Requested SUB available in $XCASUPDIR/$extractDir" 1>&2

cd $curdir

exit 0
