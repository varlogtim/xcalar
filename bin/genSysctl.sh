#!/bin/bash
set -e

DATA_FILE="$1"

count=$(( $(jq '.count' $DATA_FILE) - 1 ))

token=($(jq -r '.data[].token' $DATA_FILE))

value=($(jq -r '.data[].value' $DATA_FILE))

for idx1 in $(seq 0 $count); do
    printf "%s = %s\n" ${token[$idx1]} ${value[$idx1]}
done
