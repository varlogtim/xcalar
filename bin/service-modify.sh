#!/bin/bash

# Arguments:
# $1 - file to modify
# $2 - supervisord service name e.g. [program:something]
# $3 - substitution key
# $4 - substituted value

arg_names=("File name", \
    "Supervisord service name", \
    "Key of key-value pair", \
    "Value of key value pair")


is_empty() {
    echo  >&2 "$1 is empty."
    exit 1
}

if [ $# -lt 4 ]; then
    is_empty "${arg_names[$#]}"
fi

if [ ! -f "$1" ]; then
    echo  >&2 "File $1 does not exist."
    exit 1
fi

if ! grep -q "$2" "$1" >& /dev/null; then
    echo >&2 "Service name $2 does not exist in file $1."
    exit 1
fi

if ! grep -q "$3" "$1" >& /dev/null; then
    echo >&2 "Key name $3 does not exist in file $1."
    exit 1
fi

SECTION_LINE="$(grep -nr "$2" "$1")"
SECTION_LINE_NUM="${SECTION_LINE%%:*}"

found=0
for section in $(grep -nr "$3" "$1"); do
    KEY_LINE_NUM="${section%%:*}"

    if [ $KEY_LINE_NUM -gt $SECTION_LINE_NUM ]; then
        sed -i -e "${KEY_LINE_NUM}s/${3}=.*\$/${3}=${4}/" "$1"
        found=1
        break
    fi
done;

if [ "$found" == "0" ]; then
    echo >&2 "Key $3 not found in file $1 after service name $2."
    exit 1
fi
