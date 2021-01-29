#!/bin/bash
#
# Tests for bin/osid
# Runs 50+ tests ensuring that it returns correct values for
# the platforms we run on
#
# shellcheck disable=SC2086

_osid_expected() {
    local img
    for img in registry.int.xcalar.com/rhel7 centos:6 centos:7 registry.access.redhat.com/rhel7 oraclelinux:6 oraclelinux:7 amazonlinux:1 amazonlinux:2; do
        echo "${img}:"
        docker run -ti --rm ${img} bash -c "rpm -q \$(rpm -qf /etc/system-release) --queryformat '%{VERSION} %{NAME} %{RELEASE}\n'"
        echo
    done
}

_osid_test() {
    local img target_osid this
    this="$(readlink -f "${XLRDIR}/bin/osid")"
    local targets=(registry.access.redhat.com/rhel7 centos:6 centos:7 ubuntu:trusty oraclelinux:6 oraclelinux:7 ubuntu:bionic amazonlinux:1 ambakshi/amazon-linux amazonlinux:2 al7-build)
    local results_osid=(el7 el7 ub14 el7 ub18 amzn1 amzn1 amzn2 el7)
    local results_osid_f=(el7 el7 ub14 el7 ub18 amzn1 amzn1 amzn2 el7)
    local results_osid_a=(rhel7 centos6 centos7 ub14 oel7 ub18 amzn1 amzn1 amzn2 al7)
    local results_init=(sysvinit systemd sysvinit systemd upstart sysvinit systemd systemd sysvinit sysvinit systemd systemd)
    local results_pkg=(rpm rpm rpm rpm deb rpm rpm deb rpm rpm rpm rpm)
    local -i count=${#targets[@]} ii=0
    local -i test_idx=1
    echo "TAP version 13"
    echo "1..$((count * 5))"
    for ((ii = 0; ii < count; ii += 1)); do
        local target=${targets[$ii]}
        local cid
        if ! cid=$(docker run -d -v ${this}:/bin/osid:ro $target /bin/sleep inf); then
            echo "Bail Out!"
            exit 1
        fi

        target_osid_f=$(docker exec $cid /bin/osid -f)
        target_osid_a=$(docker exec $cid /bin/osid -a)
        target_osid=$(docker exec $cid /bin/osid)
        target_init=$(docker exec $cid /bin/osid -i)
        target_pkg=$(docker exec $cid /bin/osid -p)
        docker rm -f $cid >/dev/null
        local good_osid_f=${results_osid_f[$ii]}
        local good_osid_a=${results_osid_a[$ii]}
        local good_osid=${results_osid[$ii]}
        local good_init=${results_init[$ii]}
        local good_pkg=${results_pkg[$ii]}
        if [[ $target_osid_a == "$good_osid_a" ]]; then
            echo "$test_idx ok - $target(osid_a): $target_osid_a = $good_osid_a"
        else
            echo "$test_idx not ok - $target(osid_a): $target_osid_a should be $good_osid_a"
        fi
        test_idx=$((test_idx + 1))
        if [[ $target_osid_f == "$good_osid_f" ]]; then
            echo "$test_idx ok - $target(osid_f): $target_osid_f = $good_osid_f"
        else
            echo "$test_idx not ok - $target(osid_f): $target_osid_f should be $good_osid_f"
        fi
        test_idx=$((test_idx + 1))
        if [[ $target_osid == "$good_osid" ]]; then
            echo "$test_idx ok - $target(osid): $target_osid = $good_osid"
        else
            echo "$test_idx not ok - $target(osid): $target_osid should be $good_osid"
        fi
        test_idx=$((test_idx + 1))
        if [[ $target_init == "$good_init" ]]; then
            echo "$test_idx ok - $target(init) $target_init = $good_init"
        else
            echo "$test_idx not ok - $target(init): $target_init should be $good_init"
        fi
        test_idx=$((test_idx + 1))
        if [[ $target_pkg == "$good_pkg" ]]; then
            echo "$test_idx ok - $target(pkg): $target_pkg = $good_pkg"
        else
            echo "$test_idx not ok - $target(pkg): $target_pkg should be $good_pkg"
        fi
        test_idx=$((test_idx + 1))
    done
}

[ $# -gt 0 ] || set -- -t

case "$1" in
    -t) _osid_test ;;
    -e) _osid_expected ;;
esac
exit $?
