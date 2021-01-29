#!/bin/bash
if [ $(id -u) != 0 ]; then
    exec sudo "$0" "$@"
fi
DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
. $DIR/osid >/dev/null

test -n "${http_proxy}" && {
    $DIR/valid_url "$http_proxy" || export http_proxy
}

set -e

case "$OSID_NAME" in
    ub)
        case "$OSID_VERSION" in
            14)
                sudo apt-get update
                sudo http_proxy=${http_proxy} DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common
                sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
                sudo http_proxy=${http_proxy} apt-get update
                sudo http_proxy=${http_proxy} DEBIAN_FRONTEND=noninteractive apt-get install -y g++-5 gcc-5
                ;;
            16)
                sudo http_proxy=${http_proxy} DEBIAN_FRONTEND=noninteractive apt-get install -y g++ gcc
                ;;
            *)
                echo >&2 "Don't know how to install gcc for ${OSID_NAME}${OSID_VERSION}"
                exit 2
                ;;
        esac
        ;;
    rhel | el)
        case "$OSID_VERSION" in
            6 | 7)
                sudo http_proxy=${http_proxy} yum install -y centos-release-scl epel-release
                sudo http_proxy=${http_proxy} yum clean all
                sudo http_proxy=${http_proxy} yum makecache
                sudo http_proxy=${http_proxy} yum install -y devtoolset-7-gcc devtoolset-7-binutils devtoolset-7-gcc-c++
                ;;
            *)
                echo >&2 "Don't know how to install gcc for ${OSID_NAME}${OSID_VERSION}"
                exit 2
                ;;
        esac
        ;;
    *)
        echo >&2 "Don't know how to install gcc for ${OSID_NAME}${OSID_VERSION}"
        ;;
esac
