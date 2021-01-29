#!/bin/bash
#
# Shellcheck does static code analysis of shell scripts.
#
# Usage:
#   $ shellcheck.sh $XLRDIR/bin/build
#

test $# -eq 0 && { echo >&2 "Usage: $0 [shellcheck options] script..."; exit 1; }

docker run -v "${PWD}:${PWD}:ro" -w "$PWD" --rm koalaman/shellcheck -e SC2086 -x -s bash --color=always "$@"
