#!/bin/bash
set -e
DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
export VERSION="${VERSION:-3.6.4}"
$DIR/BuildPython.sh $VERSION
