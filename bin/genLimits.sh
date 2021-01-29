#!/bin/bash
set -e

DATA_FILE="$1"

count=$(( $(jq '.count' $DATA_FILE) - 1 ))

limit=($(jq -r '.data[].name' $DATA_FILE))

soft=($(jq -r '.data[].soft' $DATA_FILE))

hard=($(jq -r '.data[].hard' $DATA_FILE))



for idx1 in $(seq 0 $count); do
    for domain in '*' 'root'; do
        printf "%-8s%-8s%-10s%-s\n" "$domain" "soft" "${limit[$idx1]}" "${soft[$idx1]}"
        printf "%-8s%-8s%-10s%-s\n" "$domain" "hard" "${limit[$idx1]}" "${hard[$idx1]}"  
    done
done
