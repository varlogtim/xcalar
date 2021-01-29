#!/bin/bash
#
# Downloads docker-ce and all dependencies into a tar file
#
# usage: crun el7 ./docker-download.sh

set -e

OSID=$(osid)
NAME=docker-ce
VERSION=18.03.0.ce
TMPDIR="${TMPDIR:-/tmp}/$(id -u)/$$"

mkdir -p "$TMPDIR"

case "$OSID" in
    el7)
        RELEASE=1.el7.centos
        sudo curl -sL https://download.docker.com/linux/centos/docker-ce.repo -o /etc/yum.repos.d/docker-ce.repo
        sudo yum makecache fast
        sudo yumdownloader --destdir=$TMPDIR --archlist=x86_64 docker-ce-${VERSION}-${RELEASE} container-selinux device-mapper-libs libcgroup pigz systemd-units tar xz iptables
        sudo rm -f -v $TMPDIR/*.i686.rpm
        fakeroot tar cf ${NAME}-${VERSION}-${RELEASE}.tar -C $TMPDIR .
        ;;
    *)
        echo >&2 "Unsupported platform"
        exit 1
        ;;
esac
