ARG CENTOS7
FROM $CENTOS7
ARG CENTOS7

ARG REFRESHED_ON=2020-06-19-1
ENV REFRESHED_ON=$REFRESHED_ON

## Install local mirror if available
ARG EL7_MIRROR_URL=http://netstore/isos/centos/7
ENV EL7_MIRROR_URL=$EL7_MIRROR_URL
RUN cd /etc/yum.repos.d \
    && if curl -fsSL $EL7_MIRROR_URL/centos-mirror.repo -O; then echo $EL7_MIRROR_URL > /etc/yum/vars/mirrorurl \
    && rm -fv $(ls /etc/yum.repos.d/CentOS-*.repo | grep -v Base) \
    && sed -i -r '/enabled/d; s/^\[([a-z]+)]/\[\1]\nenabled=0/' CentOS-Base.repo; else yum install -y epel-release; fi; \
    echo 'exclude=kernel kernel-debug* kernel-tools* linux-firmware microcode_ctl *.i?86 *.i686' >> /etc/yum.conf \
    && echo 'sslverify=false' >> /etc/yum.conf \
    && yum update -y \
    && yum install -y polkit \
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum/*

RUN find /etc/systemd/system \
     /lib/systemd/system \
    -path '*.wants/*' \
    -not -name '*journald*' \
    -not -name '*systemd-tmpfiles*' \
    -not -name '*systemd-user-sessions*' \
    -not -name '*ssh*' \
    -not -name 'dbus.*' \
    -not -name 'polkit.*' \
    -not -name 'systemd-logind.*' \
    -not -name '*crond*' \
    -exec rm -fv \{} \; \
    && systemctl set-default multi-user.target \
    && ln -sfnv /dev/null /etc/systemd/system/sys-fs-fuse-connections.mount; \
    : >/etc/machine-id; \
    mkdir -p /etc/selinux/targeted/contexts/;\
    echo '<busconfig><selinux></selinux></busconfig>' > /etc/selinux/targeted/contexts/dbus_contexts

ENV container=docker

STOPSIGNAL SIGRTMIN+3

CMD ["/usr/sbin/init"]
