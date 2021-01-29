ARG CENTOS7
FROM $CENTOS7
MAINTAINER Xcalar, Inc. <support@xcalar.com>
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

ARG XCALAR_REPOPKG_ORG=http://repo.xcalar.net
ARG XCALAR_REPOPKG_URL=http://netstore.int.xcalar.com/infra/repo.xcalar.net

ENV XCALAR_REPOPKG_URL=${XCALAR_REPOPKG_URL}

RUN yum install -y ca-certificates curl tar bzip2 gzip yum-utils rpm-build yum \
    && curl -f http://repo.xcalar.net/rpm-deps/xcalar-deps.repo -o /etc/yum.repos.d/xcalar-deps.repo \
    && curl -f http://repo.xcalar.net/rpm-deps/xcalar-deps-common.repo -o /etc/yum.repos.d/xcalar-deps-common.repo \
    && curl -f http://repo.xcalar.net/RPM-GPG-KEY-Xcalar -o /etc/pki/rpm-gpg/RPM-GPG-KEY-Xcalar \
    && rpmkeys --import /etc/pki/rpm-gpg/RPM-GPG-KEY-Xcalar \
    && sed -i "s@${XCALAR_REPOPKG_ORG}@${XCALAR_REPOPKG_URL}@g" /etc/yum.repos.d/*.repo \
    && sed -i 's@^gpgkey=.*$@gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-Xcalar@g' /etc/yum.repos.d/xcalar-deps*.repo \
    && sed -i 's/^enabled=.*$/enabled=1/g' /etc/yum.repos.d/xcalar-deps*.repo \
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum/*

ARG XCALAR_PYTHON_VERSION
ENV XCALAR_PYTHON_VERSION=${XCALAR_PYTHON_VERSION}
ENV XCALAR_PYTHON_PACKAGE=xcalar-python36-${XCALAR_PYTHON_VERSION}.x86_64.rpm
ARG XCALAR_ARROW_VERSION
ENV XCALAR_ARROW_VERSION=${XCALAR_ARROW_VERSION}

RUN curl --create-dirs -Lf \
        -o /dist/${XCALAR_PYTHON_PACKAGE} \
        ${XCALAR_REPOPKG_URL}/rpm/xcalar-python36/${XCALAR_PYTHON_PACKAGE}

COPY docker/el7/el7-offline/yum-download.sh /
