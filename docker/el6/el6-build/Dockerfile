ARG IMAGE
FROM $IMAGE
MAINTAINER Xcalar Inc. <support@xcalar.com>

RUN yum install -y centos-release-scl curl tar bzip2 gzip epel-release \
    && yum install -y devtoolset-7-gcc devtoolset-7-binutils devtoolset-7-gcc-c++ devtoolset-7-gcc-gfortran git autoconf automake ctags libtool bzip2-devel zlib-static \
    && yum install -y libxml2-devel libxslt-devel \
    && yum install -y mawk sudo libbsd-devel libarchive3-devel libarchive3 ncurses-devel libcap-devel libaio-devel ncurses-devel openssl-devel \
    && yum install -y vim-enhanced bash-completion rpm rpm-devel rpmdevtools rpm-build yum-utils \
    && yum install -y snappy snappy-devel bison yacc xz-devel perl-Test-Harness \
    && for prog in ld ld.gold gcc g++ gfortran; do ln -sfn /opt/rh/devtoolset-7/root/usr/bin/${prog} /usr/local/bin/; done \
    && ln -s /opt/rh/devtoolset-7/root/usr/lib/gcc/x86_64-redhat-linux/7 /usr/lib/gcc/x86_64-redhat-linux/7 \
    && echo '/opt/rh/devtoolset-7/root/usr/lib64' > /etc/ld.so.conf.d/devtoolset-7.conf \
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum

RUN groupadd -r -g 999 docker \
    && groupadd -r sudo && mkdir -m 0750 -p /etc/sudoers.d && echo '%sudo ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/99-sudo && chmod 0440 /etc/sudoers.d/99-sudo \
    && sed -i -E -e 's/Defaults\s+requiretty/Defaults\t!requiretty/g' /etc/sudoers

ENV LANG=en_US.UTF-8
RUN localedef -i en_US -f UTF-8 en_US.UTF-8

RUN curl -fL https://get.docker.com/builds/Linux/x86_64/docker-1.11.2.tgz | tar zxf - -C /usr/local docker/docker && ln -sfn ../docker/docker /usr/local/bin/docker \
    && curl -f http://repo.xcalar.net/deps/ninja_1.8.2_linux_amd64.tar.gz | tar zxf - -C /usr/local/bin \
    && ln -sfn ninja-1.8.2 /usr/local/bin/ninja \
    && yum install -y http://repo.xcalar.net/xcalar-release-el6.rpm \
    && yum install --enablerepo='epel' --enablerepo='xcalar-deps' --enablerepo='xcalar-deps-common' -y autoconf-2.69 automake-1.14.1 libtool-2.4.6 boost-devel-1.65.1 protobuf-devel-3.6.1 protobuf-compiler-3.6.1 editline-devel thrift-devel-0.10.0 jemalloc-devel ccache cryptopp-static zip optcmake3 optclang5 castxml libarchive-static jansson-static bzip2-devel mapr-client jq nc java-1.8.0-openjdk-headless unixODBC-devel createrepo cyrus-sasl cyrus-sasl-lib cyrus-sasl-gssapi cyrus-sasl-md5 cyrus-sasl-devel krb5-libs krb5-devel krb5-workstation pigz pbzip2 initscripts sysvinit bc sbt \
    && yum install --enablerepo='epel' --enablerepo='xcalar-deps' --enablerepo='xcalar-deps-common' -y editline-devel unixODBC-devel libffi-static mysql-devel libxml2-static libxml2-devel xmlsec1-devel freetds-devel \
    && for prog in clang clang++ lldb; do ln -sfn /opt/clang5/bin/$prog /usr/local/bin/; done \
    && find /usr/ /lib*/ -name 'libltdl*.la' -delete \
    && ldconfig \
    && yum clean all --enablerepo='*' \
    && rm -rf /var/cache/yum \
    && find /usr/ /lib*/ \( -name 'libeditline*.la' -o -name 'libeditline*.so' -o -name 'libeditline*.so.*' \) -delete # docker_only

ARG XCALAR_PYTHON_VERSION
RUN yum install --enablerepo='xcalar-deps' --enablerepo='xcalar-deps-common' -y xcalar-python36-${XCALAR_PYTHON_VERSION} xcalar-antlr xcalar-node10 xcalar-caddy phantomjs tini

ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk.x86_64" \
    PATH="/opt/xcalar/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

CMD ["/bin/bash","-l"]

# vim: ft=Dockerfile:
