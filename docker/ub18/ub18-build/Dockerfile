ARG UB18
FROM $UB18
MAINTAINER Xcalar <info@xcalar.com>
ARG UB18

ENV container=docker

RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -yq --no-install-recommends ca-certificates curl zip tar gzip apt-transport-https gnupg locales dbus systemd && rm -rf /var/lib/apt/lists/* \
    && sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen \
    && grep -q docker /etc/group || groupadd -g 999 docker \
    && curl -L http://repo.xcalar.net/pubkey.gpg | apt-key add - \
    && curl -L http://repo.xcalar.net/xcalar-release-bionic.deb -f -o /tmp/xcalar-release.deb && dpkg -i /tmp/xcalar-release.deb && rm -f /tmp/xcalar-release.deb

ENV LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 LANGUAGE=en_US:en

RUN apt-get update -q && DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends \
        gcc sg3-utils git libaio1 libaio-dev nmap valgrind wget zip unzip g++ exuberant-ctags zlib1g-dev libeditline-dev libbsd-dev libncurses5-dev devscripts ispell ccache libssl-dev \
        libglib2.0-dev libjansson4 libjansson-dev make phantomjs jq nfs-common mysql-server libmysqlclient-dev libevent-dev dictionaries-common uuid-dev pxz xz-utils wamerican lcov \
        dpkg-dev rsync netbase net-tools iproute2 python-minimal libpython2.7 cgroup-tools \
        librpm-dev rpm flex bison unixodbc-bin unixodbc-dev vim-nox bash-completion bc sudo libarchive-dev libsnappy-dev castxml libbz2-dev libxml2-dev liblzma-dev openssh-client sysstat file docker-compose \
        libreadline5 libsybdb5 unixodbc libopenblas-base freetds-common libxml2-dev libxmlsec1-dev libkrb5-dev libxml++2.6-dev \
    && rm -rf /var/lib/apt/lists/*


RUN mkdir -p /etc/sudoers.d \
        && chmod 0750 /etc/sudoers.d \
        && echo '%sudo ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/99-sudo \
        && echo 'ALL ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers.d/99-allsudo \
        && chmod 0440 /etc/sudoers.d/* \
        && sed -r 's@^D /tmp@d /tmp@' /usr/lib/tmpfiles.d/tmp.conf > /etc/tmpfiles.d/tmp.conf

ARG XCALAR_CLANG_VERSION

RUN apt-get update -q && DEBIAN_FRONTEND=noninteractive apt-get install -yq --no-install-recommends \
        xcalar-antlr \
        xcalar-boost \
        xcalar-node10 \
        xcalar-jemalloc \
        \
        optcmake3 \
        optclang${XCALAR_CLANG_VERSION} \
        mapr-client \
        ninja-build \
        tini \
        su-exec \
        optruby2 \
        jdk1.8 \
    && rm -rf /var/lib/apt/lists/* \
    && for prog in clang clang++ lldb clang-format; do ln -sfn /opt/clang${XCALAR_CLANG_VERSION}/bin/$prog /usr/local/bin/; done \
    && curl http://repo.xcalar.net/XcalarInc_RootCA.crt --create-dirs -o /usr/local/share/ca-certificates/xcalar-ca.crt \
    && update-ca-certificates --fresh

ARG XCALAR_PYTHON_VERSION
ENV XCALAR_PYTHON_VERSION=$XCALAR_PYTHON_VERSION
RUN apt-get update \
        && export DEBIAN_FRONTEND=noninteractive \
        && curl -fL http://repo.xcalar.net/apt/xcalar-python36/xcalar-python36_${XCALAR_PYTHON_VERSION}.ub18_amd64.deb -o /tmp/xcalar-python36.deb \
        && dpkg -i /tmp/xcalar-python36.deb \
        && rm -v /tmp/xcalar-python36.deb \
        && rm -rf /var/lib/apt/lists/*

ARG XCALAR_DOCKER_VERSION
RUN curl -L http://repo.xcalar.net/deps/docker-${XCALAR_DOCKER_VERSION}.tgz | tar zxf - -C /usr/bin --strip-components=1 docker/docker

ENV JAVA_HOME=/usr/java/latest
ENV PATH=/opt/xcalar/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV XCALAR_THRIFT_VERSION=0.10.0-75

ARG XCALAR_ARROW_VERSION
ARG XCALAR_GRPC_VERSION

RUN apt-get update \
    && apt-get install -y -q --no-install-recommends \
        xcalar-thrift=${XCALAR_THRIFT_VERSION}.ub18 \
        xcalar-thrift-devel=${XCALAR_THRIFT_VERSION}.ub18 \
        cryptopp-static \
        xcalar-arrow=${XCALAR_ARROW_VERSION} \
        xcalar-arrow-libs=${XCALAR_ARROW_VERSION} \
        xcalar-grpc=${XCALAR_GRPC_VERSION} \
        xcalar-caddy \
    && rm -rf /var/lib/apt/lists/*

ARG CONTAINER_UID=1000
ENV CONTAINER_UID=$CONTAINER_UID

ARG CONTAINER_USER=xcalardev
ENV CONTAINER_USER=$CONTAINER_USER

ARG CONTAINER_HOME=/home/$CONTAINER_USER
ENV CONTAINER_HOME=$CONTAINER_HOME

ARG SHARED_CACHE=/var/opt/cache
ENV SHARED_CACHE=$SHARED_CACHE

# Late entries. These need to be rolled back into one of the yum installs higher up when we're forced to rebuild the
# images. We don't want to do it now to avoid image churn.
#RUN apt-get update \
#      && DEBIAN_FRONTEND=noninteractive apt-get install -yq sysstat \
#  && apt clean all && rm -rf /var/lib/apt/lists/*

# Workaround for docker/docker#27202, technique based on comments from docker/docker#9212
ADD docker/rootfs/ /
RUN /docker-entrypoint.sh fixups && :> /etc/machine-id

STOPSIGNAL SIGRTMIN+3

ENTRYPOINT ["/docker-entrypoint.sh"]
# Workaround for docker/docker#27202, technique based on comments from :docker/docker#9212
CMD ["/lib/systemd/systemd","--system","--log-target=journal","--target=multi-user.target"]
