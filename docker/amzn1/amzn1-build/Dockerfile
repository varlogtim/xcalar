ARG AMZN1
FROM $AMZN1
MAINTAINER Xcalar Inc. <support@xcalar.com>

ARG XCALAR_DOCKER_VERSION

RUN yum install -y epel-release curl tar bzip2 gzip ca-certificates yum-utils elfutils krb5-libs krb5-devel \
    && curl -fsSL http://repo.xcalar.net/deps/docker-${XCALAR_DOCKER_VERSION}-ce.tgz | tar zxf - -C /usr/local/bin --strip=1 docker/docker \
    && yum install --enablerepo='epel' -y gcc48 gcc48-c++ make git autoconf automake ctags libtool python27-devel bzip2-devel zlib-static createrepo libxml2-devel mawk sudo python27-pip libbsd-devel jansson-devel libarchive3-devel libarchive3 ncurses-devel libcap-devel libaio-devel ncurses-devel openssl-devel rubygems24 ruby24-devel rubygems24-devel vim-enhanced bash-completion rpm rpm-devel rpmdevtools rpm-build snappy snappy-devel libbsd-devel jq fakeroot mock python2-devel pigz pbzip2 java-1.8.0-openjdk-devel bc xmlsec1-devel freetds-devel flex bison libevent-devel cronie psutils procps-ng rsync \
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum/*

ARG XCALAR_REPOPKG_ORG=http://repo.xcalar.net
ARG XCALAR_REPOPKG_URL=http://netstore.int.xcalar.com/infra/repo.xcalar.net

RUN curl -f http://repo.xcalar.net/rpm-deps/xcalar-deps.repo | sed 's/el\$releasever/amzn1/g' > /etc/yum.repos.d/xcalar-deps.repo \
    && curl -f http://repo.xcalar.net/rpm-deps/xcalar-deps-common.repo -o /etc/yum.repos.d/xcalar-deps-common.repo \
    && curl -f http://repo.xcalar.net/RPM-GPG-KEY-Xcalar -o /etc/pki/rpm-gpg/RPM-GPG-KEY-Xcalar \
    && sed -i "s@${XCALAR_REPOPKG_ORG}@${XCALAR_REPOPKG_URL}@g" /etc/yum.repos.d/*.repo \
    && yum install --enablerepo='epel' --enablerepo='xcalar-deps' --enablerepo='xcalar-deps-common' --disableplugin=priorities -y protobuf-devel-3.6.1-86.amzn1 protobuf-compiler-3.6.1-86.amzn1 jansson-static cryptopp-static libarchive-static optcmake3 optgdb8 editline-devel mapr-client sbt ccache unixODBC unixODBC-devel  phantomjs tini su-exec ninja-build \
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum/*

ARG XCALAR_PYTHON_VERSION
RUN yum install -y --enablerepo='xcalar-deps' --enablerepo='xcalar-deps-common' --disableplugin='priorities'  ${XCALAR_REPOPKG_URL}/rpm/xcalar-python36/xcalar-python36-${XCALAR_PYTHON_VERSION}.x86_64.rpm \
        && yum clean all --enablerepo='*' \
        && rm -rf /var/cache/yum/* /var/tmp/yum*

ARG XCALAR_THRIFT_VERSION
ARG XCALAR_ARROW_VERSION
ARG XCALAR_CLANG_VERSION

RUN yum install -y --enablerepo='epel' --enablerepo='xcalar-deps' --enablerepo='xcalar-deps-common' --disableplugin='priorities' \
        optclang${XCALAR_CLANG_VERSION} \
        xcalar-antlr \
        xcalar-arrow-libs-${XCALAR_ARROW_VERSION} xcalar-arrow-${XCALAR_ARROW_VERSION} \
        xcalar-boost \
        xcalar-caddy \
        xcalar-jemalloc \
        xcalar-node10 \
        xcalar-thrift-${XCALAR_THRIFT_VERSION} xcalar-thrift-devel-${XCALAR_THRIFT_VERSION} \
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum/*

# Manually add symlinks to <arch>-amazon-<osname> from <arch>-linux-<osname> or the compiler
# fails trying to find some C++ headers. I have no idea why.
RUN ln -sfn x86_64-amazon-linux /usr/lib/gcc/x86_64-linux-gnu \
    && ln -sfn x86_64-amazon-linux /usr/include/c++/4.8.5/x86_64-linux-gnu \
    && ln -sfn /usr/lib64/liblzma.so.0 /usr/lib64/lzma.so.5 \
    && for tool in clang clang++ lldb clang-format; do ln -sfn /opt/clang${XCALAR_CLANG_VERSION:0:1}/bin/${tool} /usr/local/bin/${tool}; done

RUN groupadd -r -g 999 docker \
    && groupadd -r sudo \
    && mkdir -m 0750 -p /etc/sudoers.d \
    && echo '%sudo ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/99-sudo \
    && chmod 0440 /etc/sudoers.d/99-sudo \
    && sed -i -E -e 's/Defaults\s+requiretty/Defaults\t!requiretty/g' /etc/sudoers \
    && sed -ri 's/^session\s+required\s+pam_limits.so/#session   required   pam_limits.so/g' /etc/pam.d/*

ENV container=docker

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk PATH=/opt/xcalar/bin:$JAVA_HOME/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/sbin:/bin LANG=en_US.UTF-8

RUN rm -fv /etc/rc.d/rc3.d/* && for svc in rsyslog crond; do chkconfig $svc on; done

ARG DOCKER_ENTRYPOINT_URL=http://repo.xcalar.net/deps/de/docker-entrypoint-d51a57f2.sh
RUN curl -fL $DOCKER_ENTRYPOINT_URL -o /docker-entrypoint && chmod +x /docker-entrypoint

RUN echo 'ALL ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers.d/99-allsudo \
        && visudo -c -f /etc/sudoers.d/99-allsudo \
        && chmod 0440 /etc/sudoers.d/99-allsudo \
        && ln -sfn /usr/local/bin/docker /usr/bin/

# Late entries. These need to be rolled back into one of the yum installs higher up when we're forced to rebuild the
# images. We don't want to do it now to avoid image churn.
RUN yum install -y sysstat \
      && yum clean all --enablerepo='*' && rm -rf /var/cache/yum

ENTRYPOINT ["/docker-entrypoint"]
CMD ["/sbin/init"]
