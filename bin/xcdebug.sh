#!/bin/sh

# Xcalar debug facility
#
# works with the following artifact types:
#   1. locally copied Xcalar support bundle
#   2. remote Xcalar support bundle on AWS
#   3. local core dump
#   4. local .crash dump

TMPDIR=`mktemp /tmp/xcdebug.XXXXXX`
rm -rf $TMPDIR
mkdir $TMPDIR

XCDEBUGDIR=/var/tmp/xcdebug
mkdir -p $XCDEBUGDIR
XCDEBUGCACHE=$XCDEBUGDIR/xcasup.uploads.cache
UPDATECACHECMD="aws s3 ls --recursive s3://xcasup/uploads/ > $XCDEBUGCACHE"

# amit says ls is expensive on AWS and should be cached
if [ ! -e $XCDEBUGCACHE ]
then
    $UPDATECACHECMD
    if [ "$?" != "0" ]
    then
        echo "Please setup ASUP access on your machine (http://wiki.int.xcalar.com/mediawiki/index.php/ASUPs)" 1>&2
        rm -rf $TMPDIR
        exit 1
    fi
fi

usage()
{
    echo "Usage:" 1>&2
    echo "        DEBUG OPTIONS:" 1>&2
    echo "        xcdebug.sh -c <.crash file>" 1>&2
    echo "        xcdebug.sh -o <core file>" 1>&2
    echo "        xcdebug.sh -s <local support bundle>" 1>&2
    echo "        xcdebug.sh -r <remote support bundle id> [-d <timestamp>] # default to latest" 1>&2
    echo "\n        SEARCH OPTIONS:" 1>&2
    echo "        xcdebug.sh -l [-u <hostname>]# list remote support bundles (cached)" 1>&2
    echo "        xcdebug.sh -t # refresh cache of remote support bundles" 1>&2
    echo "        xcdebug.sh -h\n" 1>&2
    rm -rf $TMPDIR
    exit 1
}

CMD=""
DUMP=""
DUMPTYPE=""
export HOSTNAME=""
TIMESTAMP=""

while getopts "c:o:s:r:lthu:d:" opt; do
  case $opt in
      c) CMD="debug" && DUMPTYPE="crash" && DUMP=$OPTARG;;
      o) CMD="debug" && DUMPTYPE="core" && DUMP=$OPTARG;;
      s) CMD="debug" && DUMPTYPE="sub" && DUMP=$OPTARG;;
      r) CMD="debug" && DUMPTYPE="rsub" && DUMP=$OPTARG;;
      u) HOSTNAME=$OPTARG;;
      d) TIMESTAMP=$OPTARG;;
      l) CMD="ls";;
      t) CMD="refresh";;
      *) usage;;
  esac
done

if [ "$CMD" = "" ]
then
    usage
fi

if [ "$CMD" = "refresh" ]
then
    $UPDATECACHECMD
    ret=$?
    rm -rf $TMPDIR
    exit $ret
fi

listSubs()
{
    subIdToSearch=$1
    cat $XCDEBUGCACHE | grep -v '/tesla/thrift' | while read ln
    do
        subDate=`echo $ln | cut -f1,2 -d' '`
        subId=`echo $ln | sed 's/.*supportPack//' | sed 's/\.tar\.gz//'`
        subHost=`echo $ln | sed 's/.*uploads\/[0-9][0-9]*//' | cut -f2 -d/`
        doPrint=1
        if [ "$HOSTNAME" != "" ]
        then
            echo $subHost | grep -q $HOSTNAME
            if [ "$?" != "0" ]
            then
                doPrint=0
            fi
        fi
        if [ "$subIdToSearch" != "" ]
        then
            echo $subIdToSearch | grep -q $subId
            if [ "$?" != "0" ]
            then
                doPrint=0
            fi
        fi

        if [ "$doPrint" = "1" ]
        then
            echo $subDate $subId $subHost
        fi
    done
}

if [ "$CMD" = "ls" ]
then
    echo "DATE                SUBID                                HOST"
    listSubs
    rm -rf $TMPDIR
    exit 0
fi

if [ "$DUMPTYPE" = "rsub" ]
then
    listSubs $DUMP > $TMPDIR/rsubsFound
    foundCount=`wc -l $TMPDIR/rsubsFound | cut -f1 -d' '`
    if [ "$TIMESTAMP" = "" ]
    then
        TIMESTAMP=`cat $TMPDIR/rsubsFound | sort | tail -1 | cut -f1,2 -d' '`
        echo "Found $foundCount SUBs; defaulting to latest ($TIMESTAMP)" 1>&2
    fi
    fileToFetch=`cat $XCDEBUGCACHE | grep -v '/tesla/thrift' | grep $DUMP | grep "$TIMESTAMP" | sed 's/.*uploads/uploads/'`
    if [ "$fileToFetch" = "" ]
    then
        echo "Failed to find SUB for SUBID $DUMP TIMESTAMP $TIMESTAMP" 1>&2
        rm -rf $TMPDIR
        exit 1
    fi
    timeStampNoSpc=`echo $TIMESTAMP | sed 's/ /_/'`
    localFile=$XCDEBUGDIR/$timeStampNoSpc_`echo $fileToFetch | sed 's@.*/@@'`
    extractDir=`echo $fileToFetch | sed 's@.*/@@' | sed 's/\.tar\.gz//'`
    if [ ! -e $localFile ] && [ ! -d $extractDir ]
    then
        # XXX amit to fix
        # aws s3 cp s3://xcasup/$fileToFetch $localFile
        wget https://xcasup.s3.amazonaws.com/$fileToFetch -O $localFile
        if [ "$?" != "0" ]
        then
            echo "Failed to download $fileToFetch" 1>&2
            exit 1
        fi
    fi
    if [ ! -d $extractDir ]
    then
        set -x
        mkdir $extractDir
        curdir=`pwd`
        cd $extractDir
        tar -zxvf $localFile
        mv supportData/* .
        rmdir supportData
        set +x
    fi
    echo "Requested SUB available in $extractDir" 1>&2
fi
    
rm -rf $TMPDIR
exit 0
