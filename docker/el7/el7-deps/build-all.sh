#!/bin/bash
#
# CC="clang" CXX="clang++" CXXFLAGS="-fPIC -DPIC -std=c++11 -stdlib=libc++ -I/opt/clang/include/c++/v1" LDFLAGS="-L/opt/clang/lib"
cd /usr/src
set -d
set -e

install-ccache.sh && yum install -y ccache*.rpm
install-clang-from-source.sh 390 && yum install -y optclang-3.9.0*.rpm

install-python.sh 3.6.2
rm -rf /opt/xcalar
yum install -y xcalar-python36-3.6.2*.rpm

export PATH=/opt/xcalar/bin:${PATH}

/opt/xcalar/bin/pip2.7 install cython simplejson
/opt/xcalar/bin/pip2.7 install -U setuptools

for prog in clang clang++ clang-3.9 clang++-3.9 gcc g++ cc c++; do
    ln -sfn /usr/bin/ccache /usr/local/bin/$prog
done
for prog in clang++ clang; do ln -sfn /opt/clang/bin/$prog /usr/local/bin; done
export PATH=/usr/local/bin:$PATH

cd /usr/src
curl -sL https://rpm.nodesource.com/setup_6.x | bash -
yumdownloader nodejs
yum install -y nodejs*.rpm

cd /usr/src
mkdir -p /var/tmp/npm-deps
LANG=en_US.UTF-8 CCACHE_DISABLE=1 npm-package.py -p /usr/src/package.json -d /var/tmp/npm-deps -s el7
mv /var/tmp/npm-deps/*.rpm /usr/src
rm -rf /var/tmp/npm-deps

cd /usr/src
curl -sSL http://wanderinghorse.net/computing/editline/libeditline-2005.11.01.tar.bz2 | tar jxf -
cd libeditline-2005.11.01/ && ./configure --without-toc --prefix=/usr && make -k || true
cd src && g++  -g -O2 -fPIC   -I. -c -o readline.o readline.c -fpermissive && g++  -g -O2 -fPIC   -I. -c -o term.o term.c -fpermissive && cd .. && make || true
make && make install DESTDIR=/tmp/edit && cd .. && fpm -s dir -t rpm -n editline-devel -v 2005.11.01 --rpm-dist el7 -a x86_64 --rpm-autoreqprov --iteration 2 --after-install ldconfig.sh -C /tmp/edit usr && rm -rf /tmp/edit && rpm -i editline-devel*.rpm

cd /usr/src
install-boost.sh
install-jemalloc.sh
PYTHON=/opt/xcalar/bin/python3.6 install-thrift.sh
install-protobuf3.sh
install-castxml.sh
install-libarchive.sh
install-jansson.sh
install-cryptopp.sh
install-nodejs.sh
yum -y install flex
