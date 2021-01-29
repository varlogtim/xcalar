# vim: ft=Dockerfile
FROM el6-build
MAINTAINER Xcalar Inc. <support@xcalar.com>

WORKDIR /usr/src

RUN printf '#!/bin/sh\nldconfig\n' > ldconfig.sh && chmod +x ldconfig.sh

RUN curl -sL http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz | tar zxf -
RUN cd autoconf-2.69 && ./configure --prefix=/usr && make -j4
RUN cd autoconf-2.69 && make DESTDIR=/tmp/ac install && rm -rf /tmp/ac/usr/share/info/dir && cd .. && fpm -s dir -t rpm --name autoconf --version 2.69 --rpm-dist el6 -a x86_64 --rpm-autoreqprov --iteration 2 -C /tmp/ac usr && rm -rf /tmp/ac && rpm -Uvh autoconf*.rpm

RUN curl -sSL http://ftp.gnu.org/gnu/automake/automake-1.14.1.tar.gz | tar zxf -
RUN cd automake-1.14.1 && ./configure --prefix=/usr && make
RUN cd automake-1.14.1 && make DESTDIR=/tmp/am install && cd .. && rm -rf /tmp/am/usr/share/info/dir && fpm -s dir -t rpm --name automake --version 1.14.1 --rpm-dist el6 -a x86_64 --rpm-autoreqprov --iteration 2 -C /tmp/am usr && rm -rf /tmp/am && rpm -Uvh automake-1.14*.rpm

RUN curl -sSL http://ftpmirror.gnu.org/libtool/libtool-2.4.6.tar.gz | tar zxf -
RUN cd libtool-2.4.6 && ./configure --prefix=/usr && make
RUN cd libtool-2.4.6 && make DESTDIR=/tmp/lt install && cd .. && rm -rf /tmp/lt/usr/share/info/dir && fpm -s dir -t rpm --name libtool --version 2.4.6 --rpm-dist el6 -a x86_64 --rpm-autoreqprov --iteration 2 -C /tmp/lt usr && rm -rf /tmp/lt && rpm -Uvh libtool*.rpm

ADD ./bin/osid /usr/bin/

ADD ./bin/install-ccache.sh /usr/bin/
RUN install-ccache.sh
RUN rpm -Uvh ccache-*.rpm

ADD ./bin/install-python.sh /usr/bin/
ADD ./bin/install-python-package.sh /usr/bin/
RUN mkdir -p pkg src/3rd/pyodbc
RUN ls -l ./src/3rd/pyodbc
COPY ./requirements.txt /usr/src
COPY ./pkg/requirements.txt /usr/src/pkg
COPY ./src/3rd/pyodbc/pyodbc-pip-patch-0001.diff /usr/src/src/3rd/pyodbc
RUN install-python.sh 2.7.13
RUN install-python.sh 3.6.2
RUN sudo rm -rf /opt/xcalar
RUN yum install -y unixODBC-devel flex
RUN yum localinstall -y xcalar-python36-3.6.2*.rpm
ENV PATH="/opt/xcalar/bin:${PATH}"

RUN /opt/xcalar/bin/pip2.7 install cython simplejson

RUN pip2.7 install -U argparse

RUN curl -sSL http://wanderinghorse.net/computing/editline/libeditline-2005.11.01.tar.bz2 | tar jxf -
RUN cd libeditline-2005.11.01/ && ./configure --without-toc --prefix=/usr && make -k || true
RUN cd libeditline-2005.11.01/src && g++  -g -O2 -fPIC   -I. -c -o readline.o readline.c -fpermissive && g++  -g -O2 -fPIC   -I. -c -o term.o term.c -fpermissive && cd .. && make || true
RUN cd libeditline-2005.11.01 && make && make install DESTDIR=/tmp/edit && cd .. && fpm -s dir -t rpm -n editline-devel -v 2005.11.01 --rpm-dist el6 -a x86_64 --rpm-autoreqprov --iteration 2 --after-install ldconfig.sh -C /tmp/edit usr && rm -rf /tmp/edit && rpm -i editline-devel*.rpm

RUN find /usr/ /lib*/ -name 'libeditline*.so*' -exec rm -f {} \;  # docker_only
RUN ldconfig

ADD ./bin/osid /usr/bin/
ADD ./bin/install-clang-from-source.sh /usr/bin/
RUN install-clang-from-source.sh 5.0.1 && yum localinstall -y optclang5-5.0.1*.rpm

ADD ./bin/install-boost.sh /usr/bin/
RUN install-boost.sh

ADD ./bin/install-bison.sh /usr/bin/
RUN install-bison.sh

ADD ./bin/install-thrift.sh /usr/bin/
RUN install-thrift.sh

ADD ./bin/install-jemalloc.sh /usr/bin/
RUN install-jemalloc.sh

ADD ./bin/install-ccache.sh /usr/bin/
RUN install-ccache.sh

ADD ./bin/install-protobuf3.sh /usr/bin/
RUN install-protobuf3.sh

RUN curl -sL https://rpm.nodesource.com/setup_6.x | bash - && yum install -y nodejs

ENV LANG en_US.UTF-8
RUN mkdir -p /var/tmp/npm-deps
COPY ./bin/npm-package.py /usr/bin
COPY ./pkg/node/package.json /usr/src
RUN npm-package.py -p ./package.json -d /var/tmp/npm-deps -s el6
RUN mv /var/tmp/npm-deps/*.rpm /usr/src
RUN rm -rf /var/tmp/npm-deps

ADD ./bin/install-castxml.sh /usr/bin/
RUN install-castxml.sh

ADD ./bin/install-libarchive.sh /usr/bin/
RUN install-libarchive.sh

ADD ./bin/install-jansson.sh /usr/bin/
RUN install-jansson.sh

ADD ./bin/install-cryptopp.sh /usr/bin/
RUN install-cryptopp.sh

ADD ./bin/install-nodejs.sh /usr/bin
RUN install-nodejs.sh


CMD ["/bin/bash","-l"]

# vim: ft=Dockerfile:
