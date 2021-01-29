#!/bin/bash
#
# shellcheck disable=SC2086

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if test -x "$DIR"/with_container.sh; then
    exec bash -x "$DIR"/with_container.sh bash -x "$DIR"/jenkins.sh "$@"
else
    exec bash -x "$DIR"/jenkins.sh "$@"
fi
