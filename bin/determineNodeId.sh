#!/bin/bash

# Determine the list of nodeIds (numbers) associated with this host.
# This results in a space-separated string, e.g., "0 1 2 3".
determineNodeId()
{
    # Lookup hostname in config.
    hostname_f="$(hostname -f 2>/dev/null)"
    if [ $? -ne 0 ] || [ -z "$hostname_f" ]; then
        hostname_f="$(hostname 2>/dev/null)"
    fi
    # Bug Xc-6331
    # We only want to look for node numbers in the Node.<num>.Ipaddr lines where hostnames
    # and IP addresses will be found, and not in lines like Constants.XcalarRootCompletePath
    # which might have the hostname (or some variant of it) in a path or other string.
    if [ -n "$hostname_f" ]; then
        nodeId=$(grep -E '^Node\.[0-9]+\.IpAddr' "$XCE_CONFIG" | grep -E "($(hostname -s)|${hostname_f}|localhost|127\.0\.0\.1)\$" | cut -d '.' -f 2)
    else
        nodeId=$(grep -E '^Node\.[0-9]+\.IpAddr' "$XCE_CONFIG" | grep -E "($(hostname -s)|localhost|127\.0\.0\.1)\$" | cut -d '.' -f 2)
    fi
    hostId=$(hostname)
    if [ -z "$nodeId" ]; then
        # Try IP addresses.
        # This handles both EL6/Ubuntu and EL7 ifconfig output.  The former
        # prepends the IP addresses with "inet addr:", while the latter uses
        # "inet ".  The ":" delimiter is not found in EL7, but the trailing
        # "cut" command still behaves correctly.
        for ip in `ifconfig | grep "inet " | sed -e 's/^ *//' | cut -d ' ' -f 2 | cut -d ':' -f 2`
        do
            nodeId=$(grep -E '^Node\.[0-9]+\.IpAddr' "$XCE_CONFIG" | grep -E "$ip\$" | cut -d '.' -f 2)
            if [ ! -z "$nodeId" ]; then
                break
            fi
        done
    fi

    XCE_NODE_LIST="$nodeId"
    export XCE_NODE_LIST
}

nodeIdVerify ()
{
    local testId=$1

    if ! [ -e "$XCE_CONFIG" ]; then
        echo >&2 "Xcalar configuration file $XCE_CONFIG for node id verification does not exist"
        exit 1
    fi

    testIpAddr=$(grep "Node.${testId}.IpAddr" "$XCE_CONFIG" | cut -d = -f2)

    if [ -z "$testIpAddr" ]; then
        echo >&2 "Node.${testId}.IpAddr does not exist in Xcalar configuration file $XCE_CONFIG"
        exit 1
    fi

    if [ -z "$XCE_STARTUP_PING_DISABLE" ]; then
        if ! /usr/bin/ping -c 1 "$testIpAddr" >/dev/null 2>&1; then
            echo >&2 "Host $testIpAddr for node id $testId in $XCE_CONFIG does not respond to ping"
            exit 1
        fi
    fi

    for param in Port ApiPort MonitorPort; do
        if ! grep -q "Node.${testId}.${param}" "$XCE_CONFIG" >/dev/null 2>&1 ; then
            echo >&2 "Parameter Node.${testId}.${param} not found in $XCE_CONFIG"
            exit 1
        fi
    done
}
