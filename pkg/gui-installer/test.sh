#!/bin/bash

DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $DIR/tests && make -s || exit 1
sleep 5
cd $DIR
. installer-sh-lib

if [ -z "${TEST_NAME}" ]; then
    echo "1..2"
else
    echo "1..7"
fi
if [ -n "$1" ]; then
    installer="$1"
else
    installer="$(find $XLRDIR/build -name 'xcalar-*-installer' | head -1)"
    if [ -z "$installer" ]; then
        echo >&2 "No installers found in $XLRDIR/build"
        exit 1
    fi
fi
if ! test -e "$installer"; then
    echo >&2 "Installer $installer not found"
    exit 1
fi
ARGS=(--installer "$installer"
    -l xctest
    -p 22022
    -i $DIR/tests/id_rsa
    --hosts-file $DIR/tests/hosts.txt
    --priv-hosts-file $DIR/tests/hosts.txt
    --license-file $XLRDIR/src/data/XcalarLic.key)

parse_args "${ARGS[@]}"

n=0
anyfailed=0
$DIR/cluster-install.sh "${ARGS[@]}" > $TMPDIR/output 2>&1
rc=${PIPESTATUS[0]}
n=$(( $n + 1 ))
if [ $rc -eq 0 ]; then
    echo "ok $n - Install cluster"
else
    anyfailed=1
    echo "not ok $n - Install cluster"
    sed 's/^/     /' "$TMPDIR/output"
fi

sleep 5

_ssh "${priv_hosts_array[1]}" 'mountpoint -q /mnt/xcalar' > $TMPDIR/output 2>&1

rc=$?
n=$(( $n + 1 ))
if [ $rc -eq 0 ]; then
    echo "ok $n - Check /mnt/xcalar is mounted"
else
    anyfailed=1
    echo "not ok $n - Check /mnt/xcalar is mounted"
    sed 's/^/     /' "$TMPDIR/output"
fi

if [ -z "${TEST_NAME}" ]; then
    exit 0
fi
$DIR/cluster-install.sh "${ARGS[@]}" --script cluster_uninstall > $TMPDIR/output 2>&1
rc=${PIPESTATUS[0]}
n=$(( $n + 1 ))
if [ $rc -eq 0 ]; then
    echo "ok $n - Uninstall cluster"
else
    anyfailed=1
    echo "not ok $n - Uninstall cluster"
    sed 's/^/     /' "$TMPDIR/output"
fi



_ssh "${priv_hosts_array[1]}" 'test -d /opt/xcalar/xcalar-gui && exit 1 || exit 0' > $TMPDIR/output 2>&1
rc=$?
n=$(( $n + 1 ))
if [ $rc -eq 0 ]; then
    echo "ok $n - Check /opt/xcalar/xcalar-gui removal"
else
    anyfailed=1
    echo "not ok $n - Check /opt/xcalar/xcalar-gui removal"
    sed 's/^/     /' "$TMPDIR/output"
fi

_ssh "${priv_hosts_array[0]}" 'test -d /srv/xcalar && exit 0 || exit 1' > $TMPDIR/output 2>&1
rc=$?
n=$(( $n + 1 ))
if [ $rc -eq 0 ]; then
    echo "ok $n - Check /srv/xcalar present"
else
    anyfailed=1
    echo "not ok $n - Check /srv/xcalar present"
    sed 's/^/     /' "$TMPDIR/output"
fi

_ssh "${priv_hosts_array[2]}" 'test -d /var/opt/xcalar && exit 0 || exit 1' > $TMPDIR/output 2>&1
rc=$?
n=$(( $n + 1 ))
if [ $rc -eq 0 ]; then
    echo "ok $n - Check /var/opt/xcalar present"
else
    anyfailed=1
    echo "not ok $n - Check /var/opt/xcalar present"
    sed 's/^/     /' "$TMPDIR/output"
fi

_ssh "${priv_hosts_array[3]}" 'test -e /var/log/Xcalar.log && exit 1 || exit 0' > $TMPDIR/output 2>&1
rc=$?
n=$(( $n + 1 ))
if [ $rc -eq 0 ]; then
    echo "ok $n - Check /var/log/Xcalar.log removal"
else
    anyfailed=1
    echo "not ok $n - Check /var/log/Xcalar.log removal"
    sed 's/^/     /' "$TMPDIR/output"
fi


if [ $anyfailed -eq 0 ]; then
    make -C tests clean
else
    echo >&2 " Leaving instances for inspection"
fi

exit $anyfailed
