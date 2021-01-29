#!/bin/bash

export PATH=/opt/xcalar/bin:$PATH:/opt/xcalar/lib/java8/jre/bin

if [ -n "$XLRDIR" ]; then
    export PATH="$XLRDIR"/bin:$PATH
fi

declare -a CGROUPS_V1_CONTROLLERS=("memory")

MIN_TOTAL_MEM="${XCE_SQLDF_MIN_TOTAL_MEM:-$XCE_MW_MIN_TOTAL_MEM}"
SOFT_MEM_PCT="${XCE_SQLDF_SOFT_MEM_PCT:-$XCE_MW_SOFT_MEM_PCT}"
MEM_PCT="${XCE_SQLDF_MEM_PCT:-$XCE_MW_MEM_PCT}"
SVC_CNT="${XCE_SQLDF_SVC_CNT:-$XCE_MW_SVC_CNT}"
SWAPPINESS="${XCE_SQLDF_SWAPPINESS:-$XCE_MW_SWAPPINESS}"
OOM_CONTROL="${XCE_SQLDF_OOM_CONTROL:-$XCE_MW_OOM_CONTROL}"
SYSD_UNIT_NAME="${XCE_SQLDF_UNIT}"

. cgroupMgmtUtils.sh

SQLDF_ARGS=${SQLDF_ARGS:--Xmx512m -jar ${XLRDIR}/lib/xcalar-sqldf.jar -jPn -R 10000}

cgroupMode
cgroupMwSetup

exec java $SQLDF_ARGS
