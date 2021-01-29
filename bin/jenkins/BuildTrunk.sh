#!/bin/bash

set -e

source doc/env/xc_aliases
export NOCADDY=1

gitsha=`git rev-parse --short=8 HEAD`

set +e
docker images --filter=dangling=true -q | xargs -r docker rmi
numImages=`docker images -aq | wc -l`
if [ "$numImages" -ge "200" ]; then
    docker images | awk '/gui-installer/{print $3}' | xargs -r docker rmi
fi
set -e

bash -ex bin/build-installers.sh

if [ "$BUILD_MODIFY_INSTALLERS" = true ]; then
  INSTALLER_PATH=/netstore/qa/Downloads/byJob/$JOB_NAME/$BUILD_NUMBER
  SYMLINK="`dirname $INSTALLER_PATH`"/"`readlink $INSTALLER_PATH`"
  FULL_INSTALLER_PATH=`realpath $SYMLINK`
  INSTALLER="$(find $FULL_INSTALLER_PATH/prod/ -name 'xcalar-*-installer' -printf '%Ts\t%p\n' | sort -nr | head -1 | cut -f2)"
  cd "$(dirname $INSTALLER)"
  modify-installers.sh $(basename $INSTALLER)
fi

# Add new tag to graphite
if [ ! -z "$GRAPHITE" ]; then
  tagName="build${BUILD_NUMBER}"
  echo "prod.tags.$tagName.${gitsha}:1|g" | nc -w 1 -u $GRAPHITE 8125
fi

