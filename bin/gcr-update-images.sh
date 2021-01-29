#!/bin/bash

gcr_image () {
    echo "gcr.io/angular-expanse-99923/${1}"
}

PUSH=false
PULL=false
while true; do
    case "$1" in
        --push) PUSH=true; shift;;
        --pull) PULL=true; shift;;
        *) break;;
    esac
done

test $# -eq 0 && set -- centos:7 centos:6 ubuntu:trusty sequenceiq/hadoop-docker:2.7.1

if $PULL; then
    for img in "$@"; do
        docker pull "${img}"
    done
fi

if $PUSH; then
    for img in "$@"; do
        IMG_ID="$(docker images -q ${img})"
        if [ -n "$IMG_ID" ]; then
            docker tag "$img" "$(gcr_image "$img")" && \
            gcloud docker -- push "$(gcr_image "$img")"
        fi
    done
else
    for img in "$@"; do
        gcloud docker -- pull "$(gcr_image "$img")" && \
        docker tag "$(gcr_image "$img")" "$img"
    done
fi
