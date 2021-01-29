#!/bin/bash
#
# This script removes most of the startup services in the EL7 container.
# See https://hub.docker.com/_/centos/

#RUN \
    (cd /lib/systemd/system/sysinit.target.wants/ || exit 1; for i in *; do [ $i == systemd-tmpfiles-setup.service ] || rm -vf $i; done); \
    (cd /lib/systemd/system/ || exit 1; for i in systemd-machine-id-commit.service systemd-update-utmp-runlevel.service dracut*.service; do systemctl mask $i; done); \
    rm -vf /lib/systemd/system/multi-user.target.wants/*;\
    rm -vf /etc/systemd/system/*.wants/*;\
    rm -vf /lib/systemd/system/local-fs.target.wants/*; \
    rm -vf /lib/systemd/system/sockets.target.wants/*udev*; \
    rm -vf /lib/systemd/system/sockets.target.wants/*initctl*; \
    rm -vf /lib/systemd/system/basic.target.wants/*;\
    rm -vf /lib/systemd/system/anaconda.target.wants/*;\
    ln -sfnv multi-user.target /lib/systemd/system/default.target;\
    ln -sfnv /dev/null /etc/systemd/system/sys-fs-fuse-connections.mount; \
    mkdir -p /etc/selinux/targeted/contexts/;\
    echo '<busconfig><selinux></selinux></busconfig>' > /etc/selinux/targeted/contexts/dbus_contexts

#    rm -vf /lib/systemd/system/systemd-update-utmp-runlevel.*
