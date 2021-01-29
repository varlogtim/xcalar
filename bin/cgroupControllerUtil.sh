#!/bin/bash

test_mount () {
    unit_path_fs="$(findmnt -o FSTYPE -nT ${1})"

    if [ "$unit_path_fs" != "cgroup" ] && [ "$unit_path_fs" != "cgroup2" ]; then
        echo >&2 "path is not on a cgroup filesystem"
        exit 1
    fi
}

set -e

if [ "$#" != "4" ]; then
    echo >&2 "usage: $0 <command> <username|uid> <group name|gid> <cgroup path>"
    exit 1
fi

delete_controller="false"
create_controller="false"
modify_controller="false"

case "${1}" in
    create)
        create_controller="true"
        ;;
    delete)
        delete_controller="true"
        ;;
    modify)
        modify_controller="true"
        ;;
    *)
        echo >&2 "illegal command: $1, valid commands are: create, delete"
        exit 1
        ;;
esac

if ! getent passwd "${2}" >/dev/null ; then
    echo >&2 "invalid username|uid"
    exit 1
fi

if ! getent group "${3}" >/dev/null ; then
    echo >&2 "invalid group name|gid"
    exit 1
fi

unit_path="$(readlink -f ${4})"
if ! [[ "$unit_path" =~  .*/xcalar.slice/.+ ]]; then
    echo >&2 "path is not part of the xcalar.slice subtree"
    exit 1
fi

# based on the previous test xcalar.slice must exist
slice_unit_path="${unit_path%%/xcalar.slice*}/xcalar.slice"

if ! [ -d "$slice_unit_path" ]; then
    echo >&2 "path $slice_unit_path does exist"
    exit 1
fi

# now we move from leaf to root to find the deepest directory
# that actually exists. worst case: nothing below xcalar.slice
# exists and existing_unit_path == slice_unit_path
existing_unit_path="$unit_path"
while true; do
    if [ -d "$existing_unit_path" ]; then
        break;
    fi

    existing_unit_path=$(dirname "$existing_unit_path")
done

test_mount "$slice_unit_path"
test_mount "$existing_unit_path"

if [ "$create_controller" == "true" ]; then
    mkdir -p "${4}"
    chown "${2}:${3}" "${4}"
fi

if [ "$delete_controller" == "true" ]; then
    rmdir "${4}"
fi

if [ "$modify_controller" == "true" ]; then
    chown -R "${2}:${3}" "${4}"
fi
