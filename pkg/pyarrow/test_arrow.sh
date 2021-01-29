#!/bin/bash

set -e

ln -s /opt/python/cp35-cp35m/bin/ctest /usr/bin/ctest

mkdir -p /arrow/cpp/release

cd /arrow/cpp/release

cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/arrow-dist -DARROW_BUILD_TESTS=ON -DARROW_BUILD_SHARED=ON -DARROW_BOOST_USE_SHARED=ON -DARROW_JEMALLOC=ON -DARROW_RPATH_ORIGIN=ON -DARROW_JEMALLOC_USE_SHARED=OFF -DBoost_NAMESPACE=arrow_boost -DBOOST_ROOT=/arrow_boost_dist ..

ninja unittest

cd /parquet-cpp

ARROW_HOME=/arrow-dist cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/arrow-dist -DPARQUET_BUILD_TESTS=ON -DPARQUET_BUILD_SHARED=ON -DPARQUET_BUILD_STATIC=OFF -DPARQUET_BOOST_USE_SHARED=ON -DBoost_NAMESPACE=arrow_boost -DBOOST_ROOT=/arrow_boost_dist -DPARQUET_RPATH_ORIGIN=ON -GNinja .

PARQUET_TEST_DATA=/parquet-cpp/data ninja unittest
