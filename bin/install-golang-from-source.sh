#!/bin/bash
set -e

VERSION=${1:-1.9.3}
GOVERSION="go${VERSION}"
PREFIX=${PREFIX:-/usr/local}
DESTDIR=${PREFIX}/${GOVERSION}

URL=https://dl.google.com/go/${GOVERSION}.src.tar.gz

GOVERSION_BOOTSTRAP="go1.4"
URL_BOOTSTRAP="https://storage.googleapis.com/golang/${GOVERSION_BOOTSTRAP}-bootstrap-20170531.tar.gz"

export GOROOT_BOOTSTRAP=${GOROOT_BOOTSTRAP:-${PREFIX}/${GOVERSION_BOOTSTRAP}}
export GOARCH=amd64
export GOOS=linux

GOROOT_BOOTSTRAP_TMP="${GOROOT_BOOTSTRAP}.tmp"
if test -d "$GOROOT_BOOTSTRAP_TMP"; then
    rm -rf $GOROOT_BOOTSTRAP $GOROOT_BOOTSTRAP_TMP
fi

if ! test -d $GOROOT_BOOTSTRAP; then
    mkdir -p $GOROOT_BOOTSTRAP_TMP
    ln -sfn $GOROOT_BOOTSTRAP_TMP $GOROOT_BOOTSTRAP

    curl -L $URL_BOOTSTRAP | tar zxf - -C $GOROOT_BOOTSTRAP_TMP --strip-components=1
    cd $GOROOT_BOOTSTRAP/src
    GOROOT=${GOROOT_BOOTSTRAP} ./make.bash
    cd -
    rm $GOROOT_BOOTSTRAP # symlink
    mv $GOROOT_BOOTSTRAP_TMP $GOROOT_BOOTSTRAP
fi

export GOROOT=$DESTDIR
export GOROOT_FINAL=/usr/local/go
export CGO_ENABLED=0
rm -rf $GOROOT
mkdir -p $GOROOT

curl -L "$URL" | tar zxf - -C $GOROOT --strip-components=1
cd $GOROOT/src
./make.bash
ln -sfn ${GOVERSION} /usr/local/go
ln -sfn ../go/bin/go ${PREFIX}/bin/go
ln -sfn ../go/bin/gofmt ${PREFIX}/bin/gofmt

# example of building mc and installing into PREFIX
export GOPATH=$HOME/go
export PATH=/usr/local/bin:$PATH
go get -d github.com/minio/mc
cd $GOPATH/src/github.com/minio/mc
make
cp mc /usr/local/bin/
mc version
cd -
