#!/bin/bash
set -e
VERSION="${1:-3.9}"
DIR="$(cd $(dirname ${BASH_SOURCE}) && pwd)"
. "$DIR/osid" &>/dev/null

if [ "$OSID_NAME" = ub ]; then
    case "$OSID_VERSION" in
        14) code=trusty;;
        16) code=xenial;;
        *) echo >&2 "Unknown OS version $OSID_VERSION"; exit 2;;
    esac
    cat << EOF | sudo tee /etc/apt/sources.list.d/llvm.list
deb http://apt.llvm.org/$code/ llvm-toolchain-$code-$VERSION main
deb-src http://apt.llvm.org/$code/ llvm-toolchain-$code-$VERSION main
EOF
    curl -sSL http://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
    sudo http_proxy=${http_proxy} apt-get update
    sudo http_proxy=${http_proxy} DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
        clang-$VERSION \
        clang-tidy-$VERSION \
        clang-format-$VERSION \
        libclang-$VERSION-dev \
        libclang-common-$VERSION-dev \
        llvm-$VERSION \
        llvm-$VERSION-dev \
        llvm-$VERSION-runtime \
        llvm-$VERSION-tools \
        lldb-$VERSION \
        python-clang-$VERSION \
        python-lldb-$VERSION \
        libc++-dev
elif [[ "$OSID_NAME" =~ el ]]; then
    echo >&2 "Unsupport OS ${OSID_NAME}${OSID_VERSION}"
    exit 1
fi
sudo ln -sfn /usr/bin/llvm-symbolizer-$VERSION /usr/local/bin/llvm-symbolizer
sudo ln -sfn /usr/bin/clang++-${VERSION} /usr/local/bin/clang++
sudo ln -sfn /usr/bin/clang-${VERSION} /usr/local/bin/clang
