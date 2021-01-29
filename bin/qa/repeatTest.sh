#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <numIters> <testLib>"
    exit 0
fi

numIters=$1
shift

for i in `seq 1 $numIters`; do
    echo "============================================="
    echo "Running iteration $i of $numIters"
    echo "============================================="
    bash -c "$@"
    ret=$?
    if [ $ret -ne 0 ]; then
        echo "Error occured. Aborting"
        exit $ret
    fi
done


