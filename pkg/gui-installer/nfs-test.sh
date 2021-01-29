#!/bin/bash

# script calling prototype
# nfs-test.sh <cmd> <mount point> <data file> <idx>

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"

nfs_clean () {
    local path=$1
    local host=$2
    rm -f "${path}/${host}_${name_array[$host]}"
    return $?
}

nfs_test () {
    local path=$1

    for idx in `seq 0 $max_host`; do
        if [ ! -f "${path}/${idx}_${name_array[$idx]}" ]; then
            return 1
        fi

        data=$(cat "${path}/${idx}_${name_array[$idx]}")
        if [ "$data" != "${data_array[$idx]}" ]; then
            return 1
        fi
    done

    return 0
}

nfs_write () {
    local path=$1
    local host=$2
    echo "${data_array[$host]}" > "${path}/${host}_${name_array[$host]}"
    return $?
}

if [ ! -f "$3" ]; then
    exit 1;
fi

names_line=$(cat "$3" | head -1)
data_line=$(cat "$3" | head -2 | tail -1)

oldIFS=$IFS
IFS=':'
read -r -a name_array <<< "$names_line"
read -r -a data_array <<< "$data_line"
IFS=$oldIFS
max_host=$(( ${#name_array[*]}-1 ))

case $1 in
    write)
        nfs_write $2 $4
        rc=$?
        ;;
    test)
        nfs_test $2
        rc=$?
        ;;
    clean)
        nfs_clean $2 $4
        rc=$?
        ;;
esac
exit $rc

