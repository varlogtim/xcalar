#!/bin/bash

set -e

source doc/env/xc_aliases

export NOCADDY=1

bash -ex bin/build-installers.sh
