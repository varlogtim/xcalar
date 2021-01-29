#!/bin/bash
#
# This script is called by $XLRDIR/bin/modify-installers.sh
# by default unless MODIFY_SCRIPT is specified.
#
# Set ADD_PACKAGES to extra URLs of rpm/deb packages you wish
# to add to the installer and set DEL_PACKAGES to files you
# wish to delete relative to the extracted payload.
#
# The INSTALLER_DIR is either gotten from the environment or
# specified as a parameter. $PWD is used otherwise. This is
# the root of the extracted xcalar .tar.gz payload and has
# the various OS dirs (el6, el7, deb) and others such as
# test_data, etc.
#
#
set -e

if [ -z "$1" ]; then
    if [ -z "$INSTALLER_DIR" ]; then
        echo >&2 "WARNING: No directory specified. Using `pwd`"
        INSTALLER_DIR="`pwd`"
    fi
else
    INSTALLER_DIR="$1"
fi

# TODO FIXME : The are WF specific RPMs that we default to. No harm in adding them, but
# we should clean these up after the PoC.
ADD_PACKAGES="${ADD_PACKAGES-http://repo.xcalar.net/poc/wf/el6/libbsd-0.6.0-1.el6.x86_64.rpm http://repo.xcalar.net/poc/wf/el6/python-meld3-0.6.7-1.el6.x86_64.rpm}"
DEL_PACKAGES="${DEL_PACKAGES-}"

cd $INSTALLER_DIR

if [ -n "$ADD_PACKAGES" ]; then
    for pkg in $ADD_PACKAGES; do
        for dir in el7 deb unknown; do
            [[ $pkg =~ $dir ]] && break
        done
        if [ "$pkg" = unknown ]; then
            echo >&2 "WARNING: Don't know where to put $pkg. Putting it into the root of the node installer"
            wget -q "$pkg"
            continue
        fi
        echo >&2 "Downloading $pkg to $PWD/$dir"
        (cd $dir && wget -q "$pkg")
    done
fi

if [ -n "$DEL_PACKAGES" ]; then
    for pkg in $DEL_PACKAGES; do
        echo >&2 "Removing $pkg"
        rm -fv "$pkg"
    done
fi
