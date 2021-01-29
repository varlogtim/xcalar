#!/bin/bash
#
# Run an installer in 3 clean docker containers for EL6/EL7 and UB14
#
# usage (via tap/prove):
#
#   $ prove -o -c --timer $XLRDIR/bin/installer/tests/test-installers.sh $XLRDIR/build/xcalar-1.0-installer

set +e

LOGFILE=test-installers.log
truncate --size 0 $LOGFILE

say() {
    echo >&2 "$*"
}

die() {
    local rc=$1
    shift
    say "ERROR($rc): test-installers.sh: $*"
    exit $rc
}

tap_report() {
    local rc=$1 test_num=$2
    shift 2
    if [ $rc -eq 0 ]; then
        echo "ok $test_num - $*"
    else
        echo "not ok $test_num - $* returned $rc"
        sed 's/^/    /g' "$LOGFILE" >&2
        say "ERROR($rc): test-installers.sh: Failed test $test_num: $*"
    fi
}

# $1 = installer
# $2 = docker image to install into
test_installer_on() {
    local installer="$(readlink -f $1)"
    local image="$2" cid= rc=0
    local plat="$3"
    shift 3

    local args=("--start")

    echo "### Testing $installer on $image ###" | tee -a $LOGFILE
    tmpFsPctOfPhysicalMem=95
    tmpFsSizeKb=$(awk '/MemTotal/{ printf "%.0f\n", $2*'$tmpFsPctOfPhysicalMem'/100}' /proc/meminfo)
    if [ -n "$VOLUMES_FROM" ]; then
        VOLS="--volumes-from $VOLUMES_FROM"
    else
        VOLS="-v $XLRDIR:$XLRDIR"
    fi
    cid=$(docker run \
        $VOLS \
        --security-opt seccomp=unconfined \
        --ulimit core=0:0 \
        --ulimit nofile=140960:140960 \
        --ulimit nproc=140960:140960 \
        --ulimit memlock=-1:-1 \
        --ulimit stack=-1:-1 \
        --shm-size=${tmpFsSizeKb%.*}k \
        --memory-swappiness=10 \
        --tmpfs /run \
        --tmpfs /run/lock \
        --tmpfs /tmp \
        -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
        -v /bin/true:/is_container:ro \
        -e container=docker -e http_proxy=${http_proxy} -e no_proxy=${no_proxy} \
        -d \
        -P \
        -- $image)
    rc=$?
    if [ $rc -eq 0 ]; then
        sleep 2
        docker exec -w `pwd` -- $cid /bin/bash -x $installer "${args[@]}" >>"$LOGFILE" 2>&1
        rc=$?
    fi
    echo "#############################################" >> $LOGFILE
    docker rm -f $cid || true
    tap_report $rc $TEST_INDEX "Install on $image"
    TEST_INDEX=$((TEST_INDEX + 1))
    return $rc
}

registry_image() {
    local registry=${REGISTRY:-registry.int.xcalar.com}/xcalar
    local tag=${TAG:-v5}
    local image="${registry}/${1}:${tag}"
    echo "$image"
    docker pull -q "$image" >&2
}

base_image() {
    case "$1" in
        el7*) registry_image el7-systemd;;
        amzn1*) registry_image amzn1-upstart;;
        amzn2*) registry_image amzn2-systemd;;
        *) echo >&2 "Unknown OS: $1"; exit 2;;
    esac
}

test_installers() {
    local anyfailed=0
    . $XLRDIR/doc/env/xc_aliases
    setup_proxy
    TEST_INDEX=1
    PLATFORMS="${PLATFORMS:-el7 amzn2}"
    # Run installers platformwise
    for plat in $PLATFORMS; do
        case "$plat" in
            el7)
                test_installer_on $1 $(base_image $plat) $plat || anyfailed=1
                ;;
            amzn1)
                test_installer_on $1 $(base_image $plat) $plat || anyfailed=1
                ;;
            amzn2)
                test_installer_on $1 $(base_image $plat) $plat || anyfailed=1
                ;;
        esac
    done
    return $anyfailed
}

if [ $# -eq 0 ]; then
    echo >&2 "usage $0 <installer-1> ..."
    exit 2
fi
echo "TAP version 13"

ANYFAILED=0
for INSTALLER in "$@"; do
    if ! test_installers "$(readlink -f $INSTALLER)"; then
        ANYFAILED=1
    fi
done
exit $ANYFAILED
