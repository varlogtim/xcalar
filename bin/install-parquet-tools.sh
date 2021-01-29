#!/bin/bash
NAME=ParquetTools
VER=1.8.2
ITERATION="${ITERATION:-1}"
TMPDIR=${TMPDIR:-/tmp}/`id -un`/parquet-tools-${VER}
SRCDIR=${TMPDIR}/parquet-mr-apache-parquet-${VER}
DESTDIR=${TMPDIR}/rootfs
JAR=${SRCDIR}/parquet-tools/target/parquet-tools-${VER}.jar
set -e
rm -rf $TMPDIR
mkdir -p $TMPDIR $DESTDIR

curl -sSL https://github.com/apache/parquet-mr/archive/apache-parquet-${VER}.tar.gz | tar zxf - -C "${TMPDIR}"
cd ${SRCDIR}/parquet-tools

# build the .jar
mvn clean package -Plocal 

cp $JAR $DESTDIR
