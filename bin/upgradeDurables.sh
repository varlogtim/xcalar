#!/bin/bash

set -e

usage() {
    cat << EOF

This script reads in Xcalar log files and writes them out at the current IDL
SHA.

Note: all files to be upgraded MUST be under Xcalar root.

Usage: $0 [options]
    -c              : Path/filename of preprocessing binary
    -d              : Upgrade all durable data in Xcalar root
    -f <pathToMeta> : Path to the -meta file of specific log set to upgrade
    -p <logDumpCmd> : Path/filename of logdump command
    -s              : Skip backup of old files (FOR DEBUG ONLY)
    -v              : Verbose mode
    -h              : This help
EOF
}

xLog() {
    if $optVerbose
    then
        echo "$1"
    fi
}

datetime=$(date +%s)

optSkipBackup=false
optVerbose=false
optUpdateXlrRoot=false
optPreprocessingBin=""
optLogDumpCmd="${XLRDIR}/src/misc/logdump/logdump"

while getopts "c:df:hp:sv" OPTION
do
    case $OPTION in
        c)
            optPreprocessingBin=$OPTARG
            ;;
        d)
            optUpdateXlrRoot=true
            ;;
        f)
            optUpgradeFqpn=$OPTARG
            ;;
        p)
            optLogDumpCmd=$OPTARG
            ;;
        s)
            optSkipBackup=true
            ;;
        v)
            optVerbose=true
            ;;
        h)
            ;&
        ?)
            usage
            exit 0
            ;;
    esac
done

if [ $# -eq 0 ]; then
    usage
    exit
fi

shift $(($OPTIND - 1))
positionalArgs="$@"
xlrRoot=$($optLogDumpCmd --cfg DurConv.cfg --printXlrDir 2>/dev/null)
xLog "Using xcalar root: $xlrRoot"

backupData() {
    local of="xcalar_root_backup_$datetime"
    local opath="${PWD}/${of}.tar.bz2"
    xLog "Backing up $xlrRoot to $opath"
    tar -jcf "${opath}" "${xlrRoot}"
}

isEmpty() {
    local prefix="$1"

    local fqpn=$(sed -n 's/-meta$/-0/p' <<< $prefix)
    ! cat "$fqpn" | tr -d '\0' | read -n 1
}

upgradeDurable() {
    local fqpn="$1"
    local fqpn=$(basename "$fqpn" |sed -n 's/-meta$//p')
    xLog "Updating: $fqpn"
    if [ ! -z "$optPreprocessingBin" ]
    then
        local cmd="$optPreprocessingBin --cfg DurConvPreprocess.cfg --prefix $fqpn --upgrade"
        # echo "$cmd"
        $cmd
    fi

    local cmd="$optLogDumpCmd --cfg DurConv.cfg --prefix $fqpn --upgrade"
    # echo "$cmd"
    $cmd
}

if ! $optSkipBackup
then
    backupData
fi

if [ ! -z "$optUpdateXlrRoot" ]
then
    flist=$(find "$xlrRoot" -regex '.*-meta$')
    for f in $flist
    do
        if isEmpty "$f"
        then
            xLog "No update required for empty log: $f"
            continue
        fi

        upgradeDurable "$f"
    done
fi

if [ ! -z "$optUpgradeFqpn" ]
then
    upgradeDurable "$optUpgradeFqpn"
fi
