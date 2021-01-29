# vim: ft=Dockerfile
FROM centos:7
MAINTAINER Xcalar Inc. <support@xcalar.com>

RUN yum -y install epel-release
RUN yum -y install protobuf-devel libxml2-devel libxslt-devel mysql-devel libbsd-devel readline-devel gcc gcc-c++ wget rpm-build git clang make automake libtool jansson-static libaio-devel libevent-devel glib2-devel libedit-devel libicu-devel bzip2-devel ruby-devel ruby rubygems cmake3 bison flex yacc which sudo rpm-libs rpm-build-libs rpm-devel rpmlint debbuild rpm-sign mock autoconf python2-rpm-macros epel-rpm-macros rpmdevtools libstdc++-static lapack lapack-devel snappy snappy-devel perl-Digest-MD5
RUN localedef -i en_US -f UTF-8 en_US.UTF-8
ENV LANG=en_US.UTF-8
RUN gem install --no-rdoc --no-ri fpm
RUN curl -sL https://rpm.nodesource.com/setup_6.x | bash - && yum install -y nodejs

WORKDIR /usr/src

RUN sed -i -E -e 's/Defaults\s+requiretty/Defaults\t!requiretty/g' /etc/sudoers
ADD ./bin/osid /usr/bin/
ADD ./bin/install-clang-from-source.sh /usr/bin/
ADD ./bin/install-jemalloc.sh /usr/bin/
ADD ./bin/install-ccache.sh /usr/bin/
ADD ./bin/install-boost.sh /usr/bin/
ADD ./bin/install-thrift.sh /usr/bin/
ADD ./bin/install-protobuf3.sh /usr/bin/
ADD ./bin/install-castxml.sh /usr/bin/
ADD ./bin/install-cryptopp.sh /usr/bin/
ADD ./bin/npm-package.py /usr/bin/
ADD ./pkg/node/package.json /usr/src/
ADD ./docker/el7/el7-deps/build-all.sh /usr/bin/

ADD ./bin/install-libarchive.sh /usr/bin/
ADD ./bin/install-jansson.sh /usr/bin/
ADD ./pkg/requirements.txt /usr/src/
ADD ./bin/install-python.sh /usr/bin/
ADD ./bin/install-python-package.sh /usr/bin/
ADD ./bin/install-nodejs.sh /usr/bin/

RUN mkdir -p pkg src/3rd/pyodbc
RUN ls -l ./src/3rd/pyodbc
COPY ./requirements.txt /usr/src
COPY ./pkg/requirements.txt /usr/src/pkg
COPY ./src/3rd/pyodbc/pyodbc-pip-patch-0001.diff /usr/src/src/3rd/pyodbc


CMD ["/bin/bash","-l"]

# vim: ft=Dockerfile:
