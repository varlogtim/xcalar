#!/bin/bash

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
DIR="$(readlink -f $DIR)"

. $DIR/jenkinsUtils.sh

export XLRDIR=`pwd`
export PATH="$XLRDIR/bin:$PATH"
export CCACHE_BASEDIR=$XLRDIR

trap "postReviewToGerrit" EXIT

gitCheckoutIDL

# Debug build
git clean -fxd
build-clang config
build-clang coverage

# Prod build
git clean -fxd
build-clang config --enable-silent-rules --enable-debug=no --enable-coverage=no --enable-inlines=yes --enable-prof=no --enable-asserts=no
build-clang prod
ccache -s
