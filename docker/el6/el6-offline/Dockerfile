ARG IMAGE
FROM $IMAGE
MAINTAINER Xcalar, Inc. <support@xcalar.com>

RUN sed -i '/metadata_expire/d' /etc/yum.conf && echo -ne '\ntimeout=90\nretries=20\n' >> /etc/yum.conf \
    && yum install -y ca-certificates curl tar bzip2 gzip yum-utils yum epel-release \
    && curl -f http://repo.xcalar.net/rpm-deps/xcalar-deps.repo -o /etc/yum.repos.d/xcalar-deps.repo \
    && curl -f http://repo.xcalar.net/rpm-deps/xcalar-deps-common.repo -o /etc/yum.repos.d/xcalar-deps-common.repo \
    && curl -f http://repo.xcalar.net/RPM-GPG-KEY-Xcalar -o /etc/pki/rpm-gpg/RPM-GPG-KEY-Xcalar \
    && sed -i 's@^gpgkey=.*$@gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-Xcalar@g' /etc/yum.repos.d/xcalar-deps*.repo \
    && sed -i 's/^enabled=.*$/enabled=1/g' /etc/yum.repos.d/xcalar-deps*.repo \
    && yum clean all \
    && rm -rf /var/cache/yum

COPY ./yum-download.sh /

ENTRYPOINT ["/yum-download.sh"]
