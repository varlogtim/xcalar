ARG AMZN1
FROM $AMZN1
MAINTAINER Xcalar, Inc. <support@xcalar.com>

ARG XCALAR_REPOPKG_ORG=http://repo.xcalar.net
ARG XCALAR_REPOPKG_URL=http://netstore.int.xcalar.com/infra/repo.xcalar.net

ENV XCALAR_REPOPKG_URL=${XCALAR_REPOPKG_URL}


RUN echo 'exclude=kernel kernel-debug* kernel-devel* kernel-tools* linux-firmware microcode_ctl *.i?86 *.i686' >> /etc/yum.conf \
    && sed -i '/metadata_expire/d' /etc/yum.conf && echo -ne '\ntimeout=90\nretries=20\n' >> /etc/yum.conf \
    && yum install -y ca-certificates curl tar bzip2 gzip yum-utils rpm-build yum epel-release \
    && curl -f http://repo.xcalar.net/rpm-deps/xcalar-deps.repo | sed 's/el\$releasever/amzn1/g' > /etc/yum.repos.d/xcalar-deps.repo \
    && curl -f http://repo.xcalar.net/rpm-deps/xcalar-deps-common.repo -o /etc/yum.repos.d/xcalar-deps-common.repo \
    && curl -f http://repo.xcalar.net/RPM-GPG-KEY-Xcalar -o /etc/pki/rpm-gpg/RPM-GPG-KEY-Xcalar \
    && curl -fL https://packages.microsoft.com/keys/microsoft.asc -o /etc/pki/rpm-gpg/RPM-GPG-KEY-Microsoft \
    && rpmkeys --import /etc/pki/rpm-gpg/RPM-GPG-KEY-Xcalar \
    && sed -i "s@${XCALAR_REPOPKG_ORG}@${XCALAR_REPOPKG_URL}@g" /etc/yum.repos.d/*.repo \
    && sed -i 's@^gpgkey=.*$@gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-Xcalar@g' /etc/yum.repos.d/xcalar-deps*.repo \
    && sed -i 's/^enabled=.*$/enabled=1/g' /etc/yum.repos.d/xcalar-deps*.repo \
    && sed -i 's/^enabled.*$/enabled=0/g' /etc/yum/pluginconf.d/priorities.conf \
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum/*

ARG XCALAR_PYTHON_VERSION
ENV XCALAR_PYTHON_VERSION=${XCALAR_PYTHON_VERSION}

COPY ./yum-download.sh /
