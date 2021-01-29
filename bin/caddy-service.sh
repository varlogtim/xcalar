#!/bin/bash

export PATH=/opt/xcalar/bin:$PATH

if [ -n "$XLRDIR" ]; then
    export PATH="$XLRDIR"/bin:$PATH
fi

declare -a CGROUPS_V1_CONTROLLERS=("memory")

MIN_TOTAL_MEM="${XCE_CADDY_MIN_TOTAL_MEM:-$XCE_MW_MIN_TOTAL_MEM}"
SOFT_MEM_PCT="${XCE_CADDY_SOFT_MEM_PCT:-$XCE_MW_SOFT_MEM_PCT}"
MEM_PCT="${XCE_CADDY_MEM_PCT:-$XCE_MW_MEM_PCT}"
SVC_CNT="${XCE_CADDY_SVC_CNT:-$XCE_MW_SVC_CNT}"
SWAPPINESS="${XCE_CADDY_SWAPPINESS:-$XCE_MW_SWAPPINESS}"
OOM_CONTROL="${XCE_CADDY_OOM_CONTROL:-$XCE_MW_OOM_CONTROL}"
SYSD_UNIT_NAME="${XCE_CADDY_UNIT}"

. determineNodeId.sh

. cgroupMgmtUtils.sh

determineNodeId

# nodeId is a string containing the list of all the node numbers in the .cfg file
# running on the local node. this converts that string to an array
nodeIdArr=($nodeId)

rootPath="$(awk -F'=' '/^Constants.XcalarRootCompletePath/ {a=$2} END{print a}' $XCE_CONFIG)"

if [ -z "$rootPath" ]; then
    echo >&2 "Constants.XcalarRootCompletePath is not defined in $XCE_CONFIG"
    exit 1
fi

if [ -f "$rootPath"/config/authSecret ]; then
    JWT_SECRET=$(cat "$rootPath"/config/authSecret)
    export JWT_SECRET
fi

nodeIdVerify "${nodeIdArr[0]}"

if [ -n "$XCE_CLUSTER_URL" ] && ( [ "$CLUSTER_ALIAS" = "1" ] || [ "${nodeIdArr[0]}" != "0" ] ); then
    if [ "${nodeIdArr[0]}" != "0" ]; then
        XCE_LOGIN_PAGE="/assets/htmlFiles/nologin.html"
        export XCE_LOGIN_PAGE
    fi
    XCE_ACCESS_URL="${XCE_CLUSTER_URL}"
    export XCE_ACCESS_URL
fi

cgroupMode
# caddy cannot do cgroupMwSetup because NoNewPrivileges must be true

exec caddy -log stdout -conf=${XCE_CADDYFILE} -https-port=${XCE_HTTPS_PORT}
