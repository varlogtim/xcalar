ARG AMZN2
FROM $AMZN2
MAINTAINER Xcalar Inc. <support@xcalar.com>
ARG AMZN2

RUN echo 'exclude=kernel kernel-debug* kernel-tools* linux-firmware microcode_ctl *.i?86 *.i686' >> /etc/yum.conf

RUN yum install -y openssl openssl-libs curl tar bzip2 gzip ca-certificates yum-utils \
        && yum install -y gcc gcc-c++ make git autoconf automake ctags libtool bzip2-devel zlib-static createrepo libxml2-devel mawk sudo ncurses-devel libcap-devel libaio-devel ncurses-devel openssl-devel bash-completion rpm rpm-devel rpmdevtools rpm-build snappy snappy-devel jq fakeroot pigz pbzip2 bc krb5-libs krb5-devel psmisc procps-ng cronie which rsync mariadb-server java-1.8.0-openjdk-headless libcgroup libcgroup-tools initscripts libxml2-devel libxml2-devel libxslt-devel \
        && yum clean all --enablerepo='*' \
        && rm -rf /var/cache/yum

# Manually add symlinks to <arch>-amazon-<osname> from <arch>-linux-<osname> or the compiler
# fails trying to find some C++ headers. I have no idea why.
RUN ln -sfn x86_64-redhat-linux /usr/lib/gcc/x86_64-amazon-linux \
    && ln -sfn x86_64-redhat-linux /usr/include/c++/7/x86_64-amazon-linux

RUN groupmod -g 990 input \
    && groupadd -r -g 999 docker \
    && groupadd -r sudo \
    && mkdir -m 0750 -p /etc/sudoers.d \
    && echo '%sudo ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/99-sudo \
    && chmod 0440 /etc/sudoers.d/99-sudo \
    && sed -i -E -e 's/Defaults\s+requiretty/Defaults\t!requiretty/g' /etc/sudoers \
    && sed -ri 's/^session\s+required\s+pam_limits.so/#session   required   pam_limits.so/g' /etc/pam.d/*

ENV LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 container=docker

RUN echo 'export LANG=en_US.UTF-8' > /etc/profile.d/lang.sh \
    && find /etc/systemd/system \
        /lib/systemd/system \
        -path '*.wants/*' \
        -not -name '*journald*' \
        -not -name '*systemd-tmpfiles*' \
        -not -name '*systemd-user-sessions*' \
        -exec rm -fv \{} \; \
    && systemctl mask systemd-update-utmp-runlevel.service \
    && ln -sfn multi-user.target /lib/systemd/system/default.target \
    && systemctl mask systemd-remount-fs.service \
    && systemctl mask getty@tty1.service \
    && systemctl mask sys-fs-fuse-connections.mount \
    && cd /lib/systemd/system \
    && rm -fv $(ls sysinit.target.wants/*.service | grep -v systemd-tmpfile) \
    && rm -fv local-fs.target.wants/systemd-remount-fs.service \
    && cd multi-user.target.wants \
    && rm -fv getty.target


ARG XCALAR_DOCKER_VERSION
ARG XCALAR_REPOPKG_ORG=http://repo.xcalar.net
ARG XCALAR_REPOPKG_URL=http://netstore.int.xcalar.com/infra/repo.xcalar.net

RUN yum install -y ${XCALAR_REPOPKG_URL}/xcalar-release-amzn2.rpm && amazon-linux-extras install -y epel \
    && yum-config-manager --save --setopt=epel.skip_if_unavailable=true \
    && sed -i "s@${XCALAR_REPOPKG_ORG}@${XCALAR_REPOPKG_URL}@g" /etc/yum.repos.d/*.repo \
    && curl -f ${XCALAR_REPOPKG_URL}/deps/docker-${XCALAR_DOCKER_VERSION}.tgz | tar zxf - -C /usr/bin --strip-components=1 docker/docker \
    && curl -f https://storage.googleapis.com/repo.xcalar.net/xcalar-ca.crt -o /etc/pki/ca-trust/source/anchors/xcalar-ca.pem \
    && update-ca-trust \
    && yum install -y ${XCALAR_REPOPKG_URL}/deps/openblas-0.2.20-6.el7.x86_64.rpm  && yum clean all --enablerepo='*' && rm -rf /var/cache/yum

ARG XCALAR_PYTHON_VERSION
RUN yum install -y --enablerepo='xcalar-deps' --enablerepo='xcalar-deps-common' --disableplugin='priorities' ${XCALAR_REPOPKG_URL}/rpm/xcalar-python36/xcalar-python36-${XCALAR_PYTHON_VERSION}.x86_64.rpm \
        && yum clean all --enablerepo='*' \
        && rm -rf /var/cache/yum/* /var/tmp/yum*

ARG XCALAR_THRIFT_VERSION
ARG XCALAR_ARROW_VERSION
ARG XCALAR_CLANG_VERSION

RUN yum install --enablerepo='xcalar-deps-common' --enablerepo='epel' --disableplugin='priorities' -y \
    protobuf-devel-3.6.1-86.amzn1 protobuf-compiler-3.6.1 jansson-static cryptopp-static mapr-client-devel libarchive-static boost-devel freetds-devcdd2 ninja-build libbsd-devel \
    ccache libbsd-devel \
    tini su-exec \
    xcalar-arrow-${XCALAR_ARROW_VERSION} \
    xcalar-arrow-libs-${XCALAR_ARROW_VERSION} \
    xcalar-jemalloc \
    xcalar-thrift-devel-${XCALAR_THRIFT_VERSION} \
    xcalar-thrift-${XCALAR_THRIFT_VERSION} \
    optclang${XCALAR_CLANG_VERSION} optcmake3 optgdb8 xcalar-node10 editline-devel sysstat xcalar-caddy \
    && yum clean all --enablerepo='*' && rm -rf /var/cache/yum

RUN for tool in clang clang++ lldb; do ln -sfn /opt/clang${XCALAR_CLANG_VERSION:0:1}/bin/${tool} /usr/local/bin/${tool}; done \
    && ln -sfn /usr/lib64/liblzma.so.0 /usr/lib64/lzma.so.5

ENV PATH=/opt/xcalar/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

ARG CONTAINER_UID=1000
ENV CONTAINER_UID=$CONTAINER_UID

ARG CONTAINER_USER=xcalardev
ENV CONTAINER_USER=$CONTAINER_USER

ARG CONTAINER_HOME=/home/xcalardev
ENV CONTAINER_HOME=$CONTAINER_HOME

ARG SHARED_CACHE=/var/opt/cache
ENV SHARED_CACHE=$SHARED_CACHE

ADD docker/rootfs/ /
RUN /docker-entrypoint.sh fixups \
    && : >/etc/machine-id

STOPSIGNAL SIGRTMIN+3

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/usr/sbin/init"]
