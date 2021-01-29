#!/bin/bash

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
DIR="$(readlink -f $DIR)"

. $DIR/jenkinsUtils.sh

export XLRDIR=`pwd`
export PATH="$XLRDIR/bin:$PATH"
export CCACHE_BASEDIR=$PWD

trap "postReviewToGerrit" EXIT

gitCheckoutIDL

# Debug build
git clean -fxd
cmBuild config debug
cmBuild check
