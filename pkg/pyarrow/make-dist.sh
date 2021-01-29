#!/bin/bash

set -e

BOOST_VERSION=1.66
ARROW_VERSION=0.13.0
ITERATION=10

#users/abakshi/dist-05-22-1/arrow/dist/

for ii in *.tar.gz; do
    dir=$(basename $ii .tar.gz)
    test -e $dir && mv $dir ${dir}.$$
    tar zxf $ii
done

for pkg in rpm deb; do
    fpm -s dir -t $pkg --name xcalar-boost --version $BOOST_VERSION --iteration $ITERATION --prefix /opt/xcalar -f -C arrow_boost_dist
    fpm -s dir -t $pkg --name arrow-dist --version $ARROW_VERSION --iteration $ITERATION --prefix /opt/xcalar -f -C arrow-dist
done

