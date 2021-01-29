#!/bin/bash

export PATH=/opt/xcalar/scripts:$PATH

if [ -n "$XLRDIR" ]; then
    export PATH="$XLRDIR"/scripts:$PATH
fi

# Run support-generate.sh in the background when there is a coredump

# We check the pidfile for a currently executing support-generate.sh
# and don't rerun it.
check_asup ()
{
    local core_pattern=$(</proc/sys/kernel/core_pattern)
    local core_basename=""
    local core_path="$(pwd)"
    if [[ "$core_pattern" =~ ^/ ]]; then
        core_path="$(dirname $core_pattern)"
        core_basename="$(echo $(basename $core_pattern) | sed -e 's/%./*/g')"
    elif [[ "$core_pattern" =~ ^\| ]]; then
        echo >&2 "WARNING: Core pattern is set to pipe to an external program: $core_pattern"
    else # the case when core_pattern is just "core.%e.%p"
        core_basename="$(echo $(basename $core_pattern) | sed -e 's/%./*/g')"
    fi
    [ "$core_path" != "/" ] && core_path="${core_path} /"
    if [ -n "$core_basename" ] && test -n "$(find ${core_path} -mindepth 1 -maxdepth 1 -name "$core_basename" 2>/dev/null)"; then
        bash support-generate.sh
    fi
}

echo >&2 "support-generate.sh started"
check_asup
echo >&2 "support-generate.sh completed"
