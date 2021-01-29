FROM ubuntu:xenial
MAINTAINER Xcalar <info@xcalar.com>

ARG APT_PROXY

ENV UPDATED_AT 20161111

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-headless

RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -yq curl zip tar gzip apt-transport-https
RUN curl -sSL http://repo.xcalar.net/deps/ninja_1.8.2_linux_amd64.tar.gz | tar zxf - -C /usr/local/bin && ln -sfn ninja-1.8.2 /usr/local/bin/ninja \
    && curl -f http://repo.xcalar.net/deps/docker-17.03.2-ce.tgz | tar zxf - -C /usr/local && ln -sfn ../docker/docker /usr/local/bin/docker \
    && curl -f http://repo.xcalar.net/pubkey.gpg | apt-key add - \
    && curl -f http://repo.xcalar.net/xcalar-release-trusty.deb > /tmp/xcalar-release-trusty.deb && dpkg -i /tmp/xcalar-release-trusty.deb && rm -f /tmp/xcalar-release-trusty.deb


RUN apt-get update -yq && DEBIAN_FRONTEND=noninteractive apt-get install -yq --no-install-recommends gcc sg3-utils openssh-server git pmccabe fio libaio1 libaio1-dbg libaio-dev sysstat iotop nmap traceroute valgrind strace libtool m4 wget clang-5.0 zip unzip doxygen libc6-dbg iperf g++ htop exuberant-ctags zlib1g-dev libeditline-dev libbsd-dev autoconf automake libncurses5-dev devscripts ispell ccache libssl-dev libglib2.0-dev libjansson4 libjansson-dev make linux-tools-common linux-tools-generic phantomjs jq nfs-common mysql-client mysql-server libmysqlclient-dev libevent-dev dictionaries-common uuid-dev pxz xz-utils realpath wamerican lcov python-pip node-uglify dpkg-dev ant openjdk-8-jdk-headless maven
## libhdfs3 deps
RUN DEBIAN_FRONTEND=noninteractive apt-get install -yq --no-install-recommends librpm3 librpmbuild3 rpm flex bison gdb python2.7-dbg ruby ruby-dev bundler libruby unixodbc-bin unixodbc-dev vim-nox bash-completion bc sudo libcrypto++-dev libarchive-dev libsnappy-dev castxml libbz2-dev libxml2-dev liblzma-dev

## fpm deps
RUN DEBIAN_FRONTEND=noninteractive apt-get install -yq --no-install-recommends ibboost1.65.1-dev xcalar-python36 xcalar-node10 optcmake3 optclang5 optgdb8 libprotobuf-dev=3.6.1-6 protobuf-compiler=3.6.1-6 libarchive-static thrift-dev mapr-client xcalar-antlr
ENV PATH=/opt/xcalar/bin:$PATH
RUN for prog in clang clang++; do ln -sfn /opt/clang5/bin/$prog /usr/local/bin; done

RUN printf 'source %s\n\ngem %s' "'https://rubygems.org'" "'fpm'" > /tmp/Gemfile && cd /tmp && bundle install

RUN curl -o /usr/bin/gosu -fsSL "https://github.com/tianon/gosu/releases/download/1.10/gosu-$(dpkg --print-architecture)" && chmod +x /usr/bin/gosu
RUN curl -sSL 'https://github.com/just-containers/s6-overlay/releases/download/v1.21.4.0/s6-overlay-amd64.tar.gz' | tar zx -C /      # docker_only
RUN for pkg in fake-factory ipdb pytest pytest-ordering enum34 apache_log_parser pygccxml; do pip3 install -U ${pkg}; done

RUN grep -q docker /etc/group || groupadd -g 999 docker
RUN mkdir -p /etc/sudoers.d && echo '%sudo ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/99-sudo && chmod 0440 /etc/sudoers.d/99-sudo

#### Thrift
# 8b.Install the 2906 patch
#COPY ./src/3rd/thrift/thrift.xcalar-build.patch /usr/src/
#RUN cd /usr/src/thrift-0.9.2 && patch -p1 < ../thrift.xcalar-build.patch

#RUN rm -rf /usr/src/thrift-0.10.0 && curl -sSL https://www.apache.org/dist/thrift/0.10.0/thrift-0.10.0.tar.gz | tar zx -C /usr/src
#RUN cd /usr/src/thrift-0.10.0 && CC=clang CXX=clang++ PYTHON=/opt/xcalar/bin/python3 PY_PREFIX=/opt/xcalar ./configure --disable-tests --prefix=/opt/xcalar --enable-static --disable-shared --enable-boost --enable-silent-rules --disable-dependency-tracking --without-ruby --without-nodejs --disable-tutorial
#RUN cd /usr/src/thrift-0.10.0 && mkdir -p /var/tmp/thrift_rootfs && make -j V=0 DESTDIR=/var/tmp/thrift_rootfs install
#RUN cd /usr/src && rm -f thrift-dev*.deb thrift-dev*.rpm && fpm -s dir -t deb --name thrift-dev -v 0.10.0 --iteration 3 -C /var/tmp/thrift_rootfs
#RUN cd /usr/src && dpkg -i /usr/src/thrift*.deb

#RUN mkdir -p /usr/share/libs && mv /usr/lib/libhdfs3.so* /usr/share/libs && mv /usr/lib/x86_64-linux-gnu/libboost_*.so* /usr/share/libs
RUN echo 'add-auto-load-safe-path /' | tee -a /etc/gdb/gdbinit
ADD ./conf/xcalar-sysctl.conf /etc/sysctl.d/99-xcalar.conf
ADD ./conf/xcalar-limits.conf /etc/security/limits.d/99-xcalar.conf
ADD ./conf/xcalar-logrotate.conf /etc/logrotate.d/xcalar
ADD ./conf/xcalar-rsyslog.conf   /etc/rsyslog.d/42-xcalar.conf
ADD ./bin/install-protobuf3.sh /usr/local/bin/
ADD ./bin/osid  /usr/local/bin/
RUN install-protobuf3.sh

ARG CONTAINER_USER
ARG CONTAINER_UID

RUN if [ -n "$CONTAINER_USER" ] && [ -n "$CONTAINER_UID" ] && ! grep -q "^$CONTAINER_USER:x:$CONTAINER_UID:" /etc/passwd; then useradd -m -s /bin/bash -u $CONTAINER_UID $CONTAINER_USER; fi  # docker_only
RUN if [ -n "$CONTAINER_USER" ]; then usermod -aG sudo $CONTAINER_USER; fi
RUN if [ -n "$CONTAINER_USER" ]; then usermod -aG docker $CONTAINER_USER; fi
RUN if [ -n "$CONTAINER_USER" ]; then usermod -aG disk $CONTAINER_USER; fi
RUN mkdir -p /var/opt/xcalar /opt/xcalar /var/opt/xcalarTest
RUN if [ -n "$CONTAINER_USER" ]; then chown $CONTAINER_USER:$CONTAINER_USER /var/opt/xcalar /var/opt/xcalarTest /opt/xcalar ; else chmod 0777 /var/opt/xcalar /opt/xcalar /var/opt/xcalarTest; fi

EXPOSE 80 443 18552-18567 5000-5015 9090

#ENTRYPOINT ["/init"]
CMD ["/bin/bash","-l"]

