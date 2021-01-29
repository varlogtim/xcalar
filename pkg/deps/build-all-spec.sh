#!/bin/bash

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"

cd "$DIR"

RPMDIR="$HOME/rpmbuild/RPMS"
SOURCEDIR="$HOME/rpmbuild/SOURCES"

DEFINES="-D '_rpmdir $RPMDIR'"

mkdir -p $SOURCEDIR $RPMDIR
touch $SOURCEDIR/config.site  # Put common make envs here like LDFLAGS, CXXFLAGS, etc


build_spec () {
    spectool -g -R "$1"
    rpmbuild -ba "$1" --nodeps --without check --clean
    sudo rpm -Uvh $RPMDIR/*/$(basename $1 .spec)-*.rpm
}

build_spec ninja-build
build_spec rpmdevtools
build_spec m4.spec
build_spec autoconf.spec
build_spec automake.spec
build_spec libtool.spec
build_spec ccache.spec
