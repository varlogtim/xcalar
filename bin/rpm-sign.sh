#!/bin/bash

# Never print out debug from this file or the password will be exposed
set +x
set -e
DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"

if [ -z "$RPM_SIGN_PASSWORD" ]; then
    export RPM_SIGN_PASSWORD="$(cat $HOME/.gnupg/password)"
fi
"$DIR/rpm-sign.exp" "$@"
