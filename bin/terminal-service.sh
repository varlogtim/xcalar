#!/bin/bash
# /opt/xcalar/bin/terminal-service.sh

export PATH=/opt/xcalar/xcalar-gui/services/terminalServer/xcalar-wetty-bin:/opt/xcalar/bin:/opt/xcalar/scripts:$PATH

if [ -n "$XLRDIR" ]; then
    export PATH="$XLRDIR"/bin:"$XLRDIR"/scripts:$PATH
fi

if [ -n "$XLRGUIDIR" ]; then
    export PATH="$XLRGUIDIR"/xcalar-gui/services/terminalServer/xcalar-wetty-bin:$PATH
fi

declare -a CGROUPS_V1_CONTROLLERS=("memory")

MIN_TOTAL_MEM="${XCE_TERMINAL_MIN_TOTAL_MEM:-$XCE_MW_MIN_TOTAL_MEM}"
SOFT_MEM_PCT="${XCE_TERMINAL_SOFT_MEM_PCT:-$XCE_MW_SOFT_MEM_PCT}"
MEM_PCT="${XCE_TERMINAL_MEM_PCT:-$XCE_MW_MEM_PCT}"
SVC_CNT="${XCE_TERMINAL_SVC_CNT:-$XCE_MW_SVC_CNT}"
SWAPPINESS="${XCE_TERMINAL_SWAPPINESS:-$XCE_MW_SWAPPINESS}"
OOM_CONTROL="${XCE_TERMINAL_OOM_CONTROL:-$XCE_MW_OOM_CONTROL}"
SYSD_UNIT_NAME="${XCE_TERMINAL_UNIT}"

. cgroupMgmtUtils.sh

cgroupMode

exec npm start --prefix "$XLRGUIDIR"/services/terminalServer
