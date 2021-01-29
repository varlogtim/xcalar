#!/bin/bash
#
# Print hdfs-sanity host and port (hdfs://$hdfsHost:$hdfsPort)
# Can be overridden via HDFS_SANITY env var
# Default is to print the full uri (default hdfs host is HDFS_SANITY=${HDFS_SANITY})

HDFS_SANITY="${HDFS_SANITY:-hdfs-sanity:8020}"

hdfs_sanity_usage () {

    cat <<EOF >&2
    usage: $(basename $0) [options]

    Print hdfs-sanity host and port (hdfs://\$hdfsHost:\$hdfsPort)
    Can be overridden via HDFS_SANITY env var
    Default is to print the full uri (default hdfs host is HDFS_SANITY=${HDFS_SANITY})

    Options:
    -f    full hdfs uri (hdfs://\$hdfsHost:\$hdfsPort, default)
    -s    short hdfs uri (\$hdfsHost:\$hdfsPort)
    -n    hostname only (\$hdfsHost)
    -p    port only (\$hdfsPort)

    Example:

    \$ snakebite ls \$(hdfs-sanity.sh)
    ...

    \$ $(basename $0) -s
    ${HDFS_SANITY}

EOF

    exit 1
}

hdfs_sanity () {
    local hdfsHostName="${HDFS_SANITY:-hdfs-sanity:8020}"
    local hdfsPort="${hdfsHostName##*:}"
    local hdfsHost="${hdfsHostName%:*}"

    if [ "$hdfsPort" = "$hdfsHost" ]; then
        hdfsPort=8020
    fi

    if ! nc -z $hdfsHost $hdfsPort; then
        return 1
    fi
    test $# -eq 0 && set -- -f
    case "$1" in
        -f|--full) echo "hdfs://${hdfsHost}:${hdfsPort}/";;
        -s|--short) echo "${hdfsHost}:${hdfsPort}";;
        -n|--name) echo "${hdfsHost}";;
        -p|--port) echo "${hdfsPort}";;
        -h|--help) hdfs_sanity_usage;;
        *) echo >&2 "${0}: Unknown option $1"; return 1;;
    esac
}

hdfs_sanity "$@"
