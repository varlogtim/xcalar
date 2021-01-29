#!/bin/bash
set -e
DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
$DIR/BuildPython.sh 2.7.13
