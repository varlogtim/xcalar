FROM amzn1-build
MAINTAINER Xcalar Inc. <support@xcalar.com>

WORKDIR /usr/src

RUN printf '#!/bin/sh\nldconfig\n' > ldconfig.sh && chmod +x ldconfig.sh

RUN curl -sL https://sourceforge.net/projects/boost/files/boost/1.65.0/boost_1_65_0.tar.gz/download | tar zxf -
RUN mkdir -p /var/tmp/usr/include && cd boost_1_65_0 && ./bootstrap.sh && ./b2 --prefix=/usr --stagedir=/var/tmp/usr link=static variant=release threading=multi runtime-link=static stage
RUN cp -a boost_1_65_0/boost /var/tmp/usr/include
RUN fpm -s dir -t rpm --name boost --version 1.65 --rpm-dist amzn -a x86_64 --rpm-autoreqprov --iteration 1 --after-install ldconfig.sh -C /var/tmp/ usr && rm -rf /var/tmp/usr
RUN rpm -Uvh boost-*.rpm
RUN find /usr/ /lib*/ -name 'libboost*.so*' -exec rm -f {} \;   # docker_only

RUN pip install -U argparse
RUN curl -sSL "https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.bz2" | tar jxf - && cd protobuf-2.5.0 && ./configure --prefix=/usr && make V=0 -j`nproc`
RUN cd protobuf-2.5.0 && make install DESTDIR=/tmp/proto && cd .. && fpm -s dir -t rpm -n protobuf-devel -v 2.5.0 --rpm-dist amzn -a x86_64 --rpm-autoreqprov --iteration 1 --after-install ldconfig.sh -C /tmp/proto usr && rm -rf /tmp/proto && rpm -i protobuf-*.rpm

RUN curl -sSL http://wanderinghorse.net/computing/editline/libeditline-2005.11.01.tar.bz2 | tar jxf -
RUN cd libeditline-2005.11.01/ && ./configure --without-toc --prefix=/usr && make -k || true
RUN cd libeditline-2005.11.01/src && g++  -g -O2 -fPIC   -I. -c -o readline.o readline.c -fpermissive && g++  -g -O2 -fPIC   -I. -c -o term.o term.c -fpermissive && cd .. && make || true
RUN cd libeditline-2005.11.01 && make && make install DESTDIR=/tmp/edit && cd .. && fpm -s dir -t rpm -n editline-devel -v 2005.11.01 --rpm-dist amzn -a x86_64 --rpm-autoreqprov --iteration 1 --after-install ldconfig.sh -C /tmp/edit usr && rm -rf /tmp/edit && rpm -i editline-devel*.rpm

ENV THRIFT_VER=0.10.0
RUN curl -sSL "http://www.us.apache.org/dist/thrift/${THRIFT_VER}/thrift-${THRIFT_VER}.tar.gz" | tar zxf - && \
	cd thrift-${THRIFT_VER} && \
	./configure --prefix=/usr --with-cpp=yes --with-boost=yes --with-qt4=no --with-ruby=no --with-csharp=no --with-java=no --with-erlang=no --enable-tests=no --with-nodejs=no --with-lua=no --with-python=no --with-perl=no --with-php=no && \
	make -j`nproc`
RUN cd thrift-${THRIFT_VER} && make install DESTDIR=/tmp/thrift && cd .. && fpm -s dir -t rpm -n thrift-devel -v ${THRIFT_VER} --rpm-dist amzn -a x86_64 --rpm-autoreqprov --iteration 1 --after-install ldconfig.sh -C /tmp/thrift usr && rm -rf /tmp/thrift && rpm -i thrift*.rpm
## These are deleted later so we don't mess up the libraries that depends on these
RUN find /usr/ /lib*/ -name 'libthrift*.so*' -o -name 'libthrift*.la' -exec rm -f {} \; # docker_only
RUN find /usr/ /lib*/ -name 'libeditline*.so*' -exec rm -f {} \;  # docker_only
RUN ldconfig
RUN curl -sL https://bootstrap.pypa.io/get-pip.py | python27
RUN for pkg in six python-dateutil dateutils user-agents ua-parser setuptools pip zope.interface ipython py argparse fake-factory ipdb pytest pytest-ordering enum34 apache_log_parser datetime pytz xlrd psutil netifaces; do \
        fpm -s python -t rpm --python-bin /usr/bin/python27 --python-package-name-prefix python27 $pkg; done


CMD ["/bin/bash","-l"]

# vim: ft=Dockerfile:
