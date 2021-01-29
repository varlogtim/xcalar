#!/bin/bash
source /opt/rh/devtoolset-7/enable
export X_SCLS="$(scl enable devtoolset-7 'echo $X_SCLS')"
