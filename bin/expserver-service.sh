#!/bin/bash

export PATH=/opt/xcalar/bin:$PATH

if [ -n "$XLRDIR" ]; then
    export PATH="$XLRDIR"/bin:$PATH
fi

declare -a CGROUPS_V1_CONTROLLERS=("memory")

id="$1"

MIN_TOTAL_MEM="${XCE_EXPSERVER_MIN_TOTAL_MEM:-$XCE_MW_MIN_TOTAL_MEM}"
SOFT_MEM_PCT="${XCE_EXPSERVER_SOFT_MEM_PCT:-$XCE_MW_SOFT_MEM_PCT}"
MEM_PCT="${XCE_EXPSERVER_MEM_PCT:-$XCE_MW_MEM_PCT}"
SVC_CNT="${XCE_EXPSERVER_SVC_CNT:-$XCE_MW_SVC_CNT}"
SWAPPINESS="${XCE_EXPSERVER_SWAPPINESS:-$XCE_MW_SWAPPINESS}"
OOM_CONTROL="${XCE_EXPSERVER_OOM_CONTROL:-$XCE_MW_OOM_CONTROL}"
if [ "$XCE_EXPSERVER_SYSD_TEMPLATE" = "1" ]; then
    SYSD_UNIT_NAME="${XCE_EXPSERVER_UNIT/@/@$id}"
else
    SYSD_UNIT_NAME="${XCE_EXPSERVER_UNIT/@/$id}"
fi

. cgroupMgmtUtils.sh

rootPath="$(awk -F'=' '/^Constants.XcalarRootCompletePath/ {a=$2} END{print a}' $XCE_CONFIG)"

if [ -z "$rootPath" ]; then
    echo >&2 "Constants.XcalarRootCompletePath is not defined in $XCE_CONFIG"
    exit 1
fi

if ! [[ "$id" =~ ^[0-9]+$ ]]; then
    echo >&2 "Instance id $id is not a number and it must be a number"
    exit 1
fi

if [ "$id" -ge "$XCE_EXPSERVER_COUNT" ]; then
    echo >&2 "Instance id $id is greater than configured server count $XCE_EXPSERVER_COUNT"
    exit 1
fi

if [ -f "$rootPath"/config/authSecret ]; then
    JWT_SECRET=$(cat "$rootPath"/config/authSecret)
    export JWT_SECRET
fi

cgroupMode
cgroupMwSetup

IFS=',' read -r -a expPortArray <<< "$XCE_EXPSERVER_PORTS"

XCE_EXP_PORT="${expPortArray[$id]}"
export XCE_EXP_PORT
export XCE_EXP_ID="$id"

exec npm start --prefix "$XLRGUIDIR"/services/expServer
