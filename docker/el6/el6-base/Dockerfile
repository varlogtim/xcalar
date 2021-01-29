ARG IMAGE
FROM $IMAGE
MAINTAINER Xcalar Inc. <support@xcalar.com>
#
# Remove all update repos from the container. This is to get the exact same container on all nodes.
# Otherwise what can/does happen if someone rebuilds on container at a later time, they'll have diff
# versions of the packages. Since all package updates for EL are security fixes, we only upgrade when
# a new "rollup" is released. eg, 7.4 -> 7.5 -> 7.6
RUN sed -n '/^\[updates/,/^$/p' /etc/yum.repos.d/CentOS-Base.repo  > /etc/yum.repos.d/updates.repo \
    && sed -i '/^\[updates/,/^$/d' /etc/yum.repos.d/CentOS-Base.repo \
    && sed -i '/^$/d; /enabled/d' /etc/yum.repos.d/updates.repo \
    && rm -fv $(ls /etc/yum.repos.d/CentOS-*.repo | grep -v CentOS-Base) \
    && echo 'enabled=0' >> /etc/yum.repos.d/updates.repo \
    && echo 'exclude=kernel kernel-headers* kernel-debug* kernel-devel* kernel-tools* linux-firmware microcode_ctl centos-release *.i?86 *.i686' >> /etc/yum.conf

