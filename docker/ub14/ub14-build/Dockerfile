ARG IMAGE
FROM $IMAGE
MAINTAINER Xcalar <info@xcalar.com>

# Debian policy module way to prevent services from attempting to start up on install
RUN echo 'exit 101' > /usr/sbin/policy-rc.d && chmod +x /usr/sbin/policy-rc.d # docker_only
RUN groupadd --non-unique --force --gid 999 docker
RUN apt-get update -yqq && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -yqq gcc sg3-utils openssh-server git pmccabe fio sysstat iotop nmap traceroute valgrind strace libtool m4 wget zip unzip doxygen libc6-dbg iperf g++ htop exuberant-ctags zlib1g-dev libeditline-dev libbsd-dev autoconf automake libncurses5-dev devscripts ispell ccache libssl-dev libglib2.0-dev libpython2.7-dev libjansson4 make linux-tools-common linux-tools-generic phantomjs apache2 nfs-common mysql-client mysql-server libmysqlclient-dev libevent-dev dictionaries-common pxz xz-utils realpath wamerican lcov python-pip dpkg-dev gawk libxml2 libxml2-dev krb5-user debhelper librpm3 librpmbuild3 rpm flex bison gdb python2.7-dbg ruby ruby-dev ruby-bundler libruby curl vim-nox bash-completion bc libarchive-dev python-lxml libxslt1-dev libxslt1.1 libsnappy1 libsnappy-dev rsync fakeroot patchutils software-properties-common libbz2-dev liblzma-dev libdb-dev python-scipy netcat apt-transport-https unixodbc-dev liblapack-dev libblas-dev libkrb5-dev libsasl2-dev libsasl2-modules-gssapi-mit libsasl2-2 libsasl2-modules pigz pbzip2 bc
RUN add-apt-repository ppa:openjdk-r/ppa && apt-get update -q && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -yq openjdk-8-jdk && update-alternatives --config java && update-alternatives --config javac
RUN curl -fL http://repo.xcalar.net/pubkey.gpg | apt-key add -
ARG XCALAR_DOCKER_VERSION
RUN curl -fL http://repo.xcalar.net/deps/docker-${XCALAR_DOCKER_VERSION}-ce.tgz | tar zxf - -C /usr/local && ln -sfn ../docker/docker /usr/local/bin/docker # docker_only
RUN curl -fL http://repo.xcalar.net/xcalar-release-trusty.deb > /tmp/xcalar-release-trusty.deb && dpkg -i /tmp/xcalar-release-trusty.deb && rm -f /tmp/xcalar-release-trusty.deb

RUN apt-get update -yqq && DEBIAN_FRONTEND=noninteractive apt-get install -yqq --no-install-recommends libboost1.65.1-dev thrift-dev=0.10.0-1 libjemalloc-dev libprotobuf-dev=3.6.1-85 protobuf-compiler=3.6.1-85 ccache castxml mapr-client sbt optclang5 optcmake3 lsb-core cryptopp-static freetds-dev libmysqlclient-dev libopenblas-base ninja-build jansson-static jq \
    && for prog in clang clang++ lldb; do ln -sfn /opt/clang5/bin/$prog /usr/local/bin/; done \
    && rm -rf /var/lib/apt/lists/*

ARG XCALAR_PYTHON_VERSION
RUN apt-get update \
        && export DEBIAN_FRONTEND=noninteractive \
        && apt-get install -y -q libreadline5 libsybdb5 unixodbc libopenblas-base freetds-common \
        && curl -fL http://repo.xcalar.net/apt/xcalar-python36/xcalar-python36_${XCALAR_PYTHON_VERSION}_amd64.deb -o /tmp/xcalar-python36.deb \
        && dpkg -i /tmp/xcalar-python36.deb \
        && rm -v /tmp/xcalar-python36.deb \
        && rm -rf /var/lib/apt/lists/*

#ARG XCALAR_PYTHON_VERSION
#RUN apt-get update \
#    && curl -f -o xcalar-python36.deb http://repo.xcalar.net/apt/xcalar-python36/pool/main/x/xcalar-python36/xcalar-python36_${XCALAR_PYTHON_VERSION}_amd64.deb \
#    && (dpkg -i xcalar-python36.deb || apt-get install -f -y || exit 1) \
#    && rm xcalar-python36.deb \
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -yq xcalar-node10 xcalar-antlr xcalar-caddy tini su-exec \
    && rm -rf /var/lib/apt/lists/*

RUN sed -i -E -e 's/Defaults\s+requiretty/Defaults\t!requiretty/g' /etc/sudoers # docker_only

## Remove .so's of libs that we depend on but don't ship with the distro.
## Since we don't dynamically link against these anymore, we don't need to worry about shipping their packages.
RUN find /usr/ /lib*/ \( -name 'libeditline*.la' -o -name 'libeditline*.so' -o -name 'libeditline*.so.*' \) -delete # docker_only
RUN echo 'add-auto-load-safe-path /' | tee -a /etc/gdb/gdbinit
#
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/99-sudo \
    && echo 'ALL ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers.d/99-allsudo \
    && chmod 0440 /etc/sudoers.d/*

RUN mkdir -p /usr/lib64/ccache && for ii in cc c++ gcc g++ clang clang++; do ln -sfvn $(command -v ccache) /usr/lib64/ccache/${ii}; done

ARG XCALAR_ARROW_VERSION
RUN rm -rf /var/lib/apt/lists && apt-get update -q && DEBIAN_FRONTEND=noninteractive apt-get install -y xcalar-arrow=${XCALAR_ARROW_VERSION} xcalar-arrow-libs=${XCALAR_ARROW_VERSION}

RUN localedef -i en_US -f UTF-8 en_US.UTF-8
ENV LANG=en_US.UTF-8 JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 PATH=/opt/xcalar/bin:$PATH

ARG CONTAINER_UID=1000
ARG CONTAINER_GID=1000
ARG CONTAINER_USER=xcalardev
ARG CONTAINER_HOME=/home/xcalardev
ARG DOCKER_ENTRYPOINT_URL=http://repo.xcalar.net/deps/de/docker-entrypoint-d51a57f2.sh
RUN curl $DOCKER_ENTRYPOINT_URL -o /docker-entrypoint && chmod +x /docker-entrypoint && bash -x /docker-entrypoint /bin/bash -c "echo Done"

ENTRYPOINT ["/docker-entrypoint"]

CMD ["/sbin/init"]
