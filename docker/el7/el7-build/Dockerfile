# syntax = docker/dockerfile:1.0-experimental
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
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum/*

RUN yum install -y initscripts sudo curl gzip yum-utils ca-certificates pam && yum clean all --enablerepo='*' && rm -rf /var/cache/yum \
    && groupadd --system --non-unique -g 999 docker  \
    && groupadd --system sudo && mkdir -m 0750 -p /etc/sudoers.d && echo '%sudo ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/99-sudo && chmod 0440 /etc/sudoers.d/99-sudo

RUN yum install -y  \
        git224-core libxml2-devel mysql-devel \
        readline-devel gcc gcc-c++ wget rpm-build make \
        libaio-devel libevent-devel glib2-devel libedit-devel \
        libcurl-devel libunwind-devel libicu-devel zip unzip bzip2-devel ruby-devel \
        ruby rubygems rubygem-bundler libcap-devel \
        which xz-devel libstdc++-static rpmdevtools libxslt-devel \
        snappy snappy-devel nmap-ncat unixODBC-devel createrepo \
        vim-enhanced psmisc cyrus-sasl cyrus-sasl-gssapi cyrus-sasl-md5 \
        cyrus-sasl-devel krb5-devel krb5-workstation  \
        bc xmlsec1-devel mysql psmisc \
        mariadb-server rsync xz-compat-libs \
        bash-completion cronie psutils procps-ng \
        libcgroup-tools sysstat polkit vte-profile \
    && yum install -y libbsd-devel jq pigz pbzip2 freetds-devel fakeroot \
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum/* /var/tmp/yum*

ARG XCALAR_REPOPKG_ORG=http://repo.xcalar.net
ARG XCALAR_REPOPKG_URL=http://netstore.int.xcalar.com/infra/repo.xcalar.net
# XD's test engier (puppeteer) dependency packages
RUN yum install -y Xvfb pango.x86_64 libXcomposite.x86_64 libXcursor.x86_64 libXdamage.x86_64 libXext.x86_64 libXi.x86_64 libXtst.x86_64 cups-libs.x86_64 libXScrnSaver.x86_64 libXrandr.x86_64 GConf2.x86_64 alsa-lib.x86_64 atk.x86_64 gtk3.x86_64 ipa-gothic-fonts xorg-x11-fonts-100dpi xorg-x11-fonts-75dpi xorg-x11-utils xorg-x11-fonts-cyrillic xorg-x11-fonts-Type1 xorg-x11-fonts-mc \
    && curl -fssL http://repo.xcalar.net/rpm-deps/google-chrome.repo | tee /etc/yum.repos.d/google-chrome.repo \
    && curl -fsSO https://dl.google.com/linux/linux_signing_key.pub \
    && rpm --import linux_signing_key.pub \
    && rm linux_signing_key.pub \
    && yum install -y google-chrome-stable \
    && yum localinstall -y "${XCALAR_REPOPKG_URL}/deps/chromedriver-2.34-2.el7.x86_64.rpm" \
    && yum install -y Xvfb \
    && yum clean all \
    && rm -rf /var/cache/yum/* /var/tmp/yum* \
    && rm -fv /etc/yum.repos.d/rpm-deps/google-chrome.repo

ARG XCALAR_DOCKER_VERSION

RUN curl -fsSL ${XCALAR_REPOPKG_URL}/deps/docker-${XCALAR_DOCKER_VERSION}.tgz | tar zxf - -C /usr/bin --strip-components=1 docker/docker \
    && curl -L -f ${XCALAR_REPOPKG_URL}/deps/docker-completion.bash -o /usr/share/bash-completion/completions/docker --create-dirs

ARG XCALAR_CLANG_VERSION

RUN curl -f ${XCALAR_REPOPKG_URL}/xcalar-ca.crt -o /etc/pki/ca-trust/source/anchors/xcalar-ca.pem \
    && update-ca-trust extract \
    && curl -f ${XCALAR_REPOPKG_URL}/rpm-deps/xcalar-deps.repo -o /etc/yum.repos.d/xcalar-deps.repo \
    && curl -f ${XCALAR_REPOPKG_URL}/rpm-deps/xcalar-deps-common.repo -o /etc/yum.repos.d/xcalar-deps-common.repo \
    && curl -f ${XCALAR_REPOPKG_URL}/RPM-GPG-KEY-Xcalar -o /etc/pki/rpm-gpg/RPM-GPG-KEY-Xcalar \
    && sed -i "s@${XCALAR_REPOPKG_ORG}@${XCALAR_REPOPKG_URL}@g" /etc/yum.repos.d/xcalar*.repo \
    && yum install --enablerepo='xcalar*' -y tini su-exec ninja-build mapr-client sbt wamerican optcmake3 castxml ccache \
            optclang${XCALAR_CLANG_VERSION} xcalar-node10 xcalar-antlr xcalar-caddy xcalar-jemalloc jansson-static cryptopp-static editline-devel \
            boost-devel optgdb8 optmaven3 \
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum/*

# ENG-10558 - phantomjs install by RPM is broken
RUN yum install -y glibc fontconfig freetype freetype-devel fontconfig-devel wget bzip2 \
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum/* \
    && curl -f ${XCALAR_REPOPKG_URL}/deps/phantomjs-2.1.1-linux-x86_64.tar.bz2 | tar xjf - -C /usr/local/share \
    && ln -sf /usr/local/share/phantomjs-2.1.1-linux-x86_64/bin/phantomjs /usr/bin

RUN echo "Post-install links" \
    && for prog in llvm-cov llvm-profdata clang clang++ lldb clang-format; do ln -sfn /opt/clang${XCALAR_CLANG_VERSION:0:1}/bin/$prog /usr/local/bin/; done \
    && for prog in gdb gdbserver; do ln -sfn /opt/gdb8/bin/$prog /usr/local/bin/; done \
    && ln -s /usr/include/editline/readline.h /usr/include/editline.h \
    && rpmdev-setuptree

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN localedef -i en_US -f UTF-8 en_US.UTF-8

ENV JAVA_HOME=/usr/java/latest PATH=/opt/xcalar/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/sbin:/bin:${JAVA_HOME}/bin

ARG LIBARCHIVE_VERSION
ARG XCALAR_THRIFT_VERSION=0.10.0-28
ARG XCALAR_ARROW_VERSION

RUN yum install -y --enablerepo='xcalar-*' --disableplugin='priorities' xcalar-thrift-${XCALAR_THRIFT_VERSION} xcalar-thrift-devel-${XCALAR_THRIFT_VERSION} xcalar-arrow-libs-${XCALAR_ARROW_VERSION} xcalar-arrow-${XCALAR_ARROW_VERSION} \
        jdk1.8 docker-compose vault awscliv2 \
        libarchive-static-${LIBARCHIVE_VERSION} \
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum/*

ARG XCALAR_GRPC_VERSION
ARG XCALAR_PYTHON_VERSION
RUN yum localinstall -y ${XCALAR_REPOPKG_URL}/rpm/xcalar-python36/xcalar-python36-${XCALAR_PYTHON_VERSION}.x86_64.rpm \
    && rpm -q xcalar-python36 \
    && yum install -y --enablerepo='xcalar-*' xcalar-grpc-${XCALAR_GRPC_VERSION} \
    && rpm -q xcalar-grpc \
    && yum clean all --enablerepo='*' \
    && rm -rfv /var/cache/yum/* /var/tmp/yum*

ARG CONTAINER_UID=1000
ENV CONTAINER_UID=$CONTAINER_UID

ARG CONTAINER_GID=1000
ENV CONTAINER_GID=$CONTAINER_GID

ENV UGID=${CONTAINER_UID}:${CONTAINER_GID}

ARG CONTAINER_USER=xcalardev
ENV CONTAINER_USER=$CONTAINER_USER

ARG CONTAINER_HOME=/home/xcalardev
ENV CONTAINER_HOME=$CONTAINER_HOME

ARG SHARED_CACHE=/var/opt/cache
ENV SHARED_CACHE=$SHARED_CACHE

ADD docker/rootfs/ /
RUN XTRACE=1 /docker-entrypoint.sh fixups

ENTRYPOINT ["/docker-entrypoint.sh"]

ENV container=docker

STOPSIGNAL SIGRTMIN+3

EXPOSE 8443 9515

CMD ["/usr/sbin/init"]
