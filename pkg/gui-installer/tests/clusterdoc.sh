#!/bin/bash

(("$#" != "1")) && echo "Usage: "`basename $0`" <cmd>" && exit 1

for c in $(seq 1 4); do
    echo "Output for container target$c"
    docker exec target$c /bin/sh -c "$@"
    echo
done
