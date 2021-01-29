#!/bin/bash
# generates to $XCE_CGCONFIG_PATH the cgconfig conf file necessary to setup our cgroups
# takes 3 parameters for user and group to set cgroups perms to or defaults to "xcalar" and config file to write to
#
# shellcheck disable=SC2206,SC22153

if test -r /etc/xcalar/default; then
    . /etc/xcalar/default
fi
XCE_USER=${XCE_USER:-xcalar}
XCE_GROUP=${XCE_GROUP:-xcalar}

CGROUP_USER=${1:-$XCE_USER}
CGROUP_GROUP=${2:-$XCE_GROUP}
if [ -n "$3" ]; then
    CGCONFIG_PATH="$3"
else
    if [ -d /etc/cgconfig.d ]; then
        CGCONFIG_PATH=/etc/cgconfig.d/xcalar-${CGROUP_USER}.conf
    else
        CGCONFIG_PATH=/etc/cgconfig.conf
    fi
fi
CGCONFIG_RESTART=0

DIR="$(cd "$(dirname ${BASH_SOURCE[0]})" && pwd)"

say() {
    echo >&2 "$1"
}

validate_cgroup_users() {
    if ! getent passwd $CGROUP_USER >/dev/null; then
        say "cgroups-setup: Unable to find user $CGROUP_USER"
        exit 1
    fi
    if ! getent group $CGROUP_GROUP >/dev/null; then
        say "cgroups-setup: Unable to find group $CGROUP_GROUP"
        exit 1
    fi
}

is_cgroup_supported() {
    if ! grep -q ^cgroup /proc/mounts; then
        return 1
    fi
}

generate_cgroup_targets() {
    local cgroup_childnode_classes=('usr_xpus' 'sys_xpus')
    local cgroup_childnode_schedulers=('sched0' 'sched1' 'sched2')
    local xce_service='xcalar-usrnode.service'
    local xce_slice='xcalar.slice'
    local cgroup_xcalar_xce="xcalar_xce_${CGROUP_USER}"
    local cgroup_xcalar_mw="xcalar_middleware_${CGROUP_USER}"

    ROOT_CGROUP_TARGETS=()
    USER_CGROUP_TARGETS=()

    local slice="${xce_slice}"
    local service="${slice}/${xce_service}"
    ROOT_CGROUP_TARGETS+=("$slice")
    USER_CGROUP_TARGETS+=("${service}")
    USER_CGROUP_TARGETS+=("${service}/xcalar-usrnode.scope")
    for class in "${cgroup_childnode_classes[@]}"; do
        for sched in "${cgroup_childnode_schedulers[@]}"; do
            USER_CGROUP_TARGETS+=("${service}/${class}-${sched}.scope")
        done
    done

    USER_CGROUP_TARGETS+=(
        ${cgroup_xcalar_xce}
        ${cgroup_xcalar_mw}
    )
}

generate_cgconfig_xcalar () {
    CGCONFIG_MD5="0"
    if [ -e $CGCONFIG_PATH ]; then
        CGCONFIG_MD5="$(md5sum $CGCONFIG_PATH | cut -d' ' -f1)"
        sed -i "/^## xcalar-cgroup-start/,/^## xcalar-cgroup-end/d" $CGCONFIG_PATH
        sed -i "/^## xcalar-cgroup-start-$CGROUP_USER/,/^## xcalar-cgroup-end-$CGROUP_USER/d" $CGCONFIG_PATH
    fi
    echo "## xcalar-cgroup-start-$CGROUP_USER" >> $CGCONFIG_PATH
    generate_cgroup_targets

    for t in ${ROOT_CGROUP_TARGETS[*]}; do
        cat >> $CGCONFIG_PATH << EOF
group ${t} {
    perm {
            admin {
                    uid = root;
                    gid = root;
            }
            task {
                    uid = root;
                    gid = root;
            }
    }
    cpu { }
    cpuacct { }
    cpuset { }
    memory { }
}
EOF
    done

    for t in ${USER_CGROUP_TARGETS[*]}; do
        cat >> $CGCONFIG_PATH << EOF
group ${t} {
    perm {
            admin {
                    uid = ${CGROUP_USER};
                    gid = ${CGROUP_GROUP};
            }
            task {
                    uid = ${CGROUP_USER};
                    gid = ${CGROUP_GROUP};
            }
    }
    cpu { }
    cpuacct { }
    cpuset { }
    memory { }
}
EOF
    done
    echo "## xcalar-cgroup-end-$CGROUP_USER" >> $CGCONFIG_PATH
    if [ "$CGCONFIG_MD5" != "$(md5sum $CGCONFIG_PATH | cut -d' ' -f1)" ]; then
        CGCONFIG_RESTART=1
    fi
}

enable_cgconfig () {
    osid="$($DIR/osid)"
    case "$osid" in
        *el6|amzn1)
            chkconfig cgconfig on
            if ((CGCONFIG_RESTART)); then
                service cgconfig restart
            else
                service cgconfig status || service cgconfig start
            fi
            ;;
        *el7|ub16|ub18|amzn2)
            if ((CGCONFIG_RESTART)); then
                systemctl enable cgconfig.service
                systemctl restart cgconfig.service
            else
                systemctl enable --now cgconfig.service
            fi
            ;;
    esac
}

if [ $(id -u) -ne 0 ]; then
    say "ERROR: Must be root to run ${BASH_SOURCE[0]}"
    exit 1
fi

if ! is_cgroup_supported; then
    say "Cgroups not supported or cgconfig not installed"
    exit 0
fi

validate_cgroup_users

generate_cgconfig_xcalar

enable_cgconfig
