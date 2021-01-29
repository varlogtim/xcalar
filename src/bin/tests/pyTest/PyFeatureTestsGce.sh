#!/bin/bash

#
# Runs pyTests on GCE cluster. Must be executed on node 0 of cluster.
#

export XLRPY_MGMTD_URL="http://localhost:9090/thrift/service/XcalarApiService/"
export XcalarQaDatasetPath="/netstore/datasets"
# Use /mnt/xcalar as shared scratch space because it will get deleted on cluster
# teardown.
export XcalarQaScratchShare="/mnt/xcalar/qaScratch"

cd /tmp/pyTest/pyTest
py.test -vv -rw -x --durations=10 -m "not nogce" ./
