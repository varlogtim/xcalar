#!/bin/bash

UBI7_COMPAT=${UBI7_COMPAT:-0}

fix_systemd() {
    # Don't start any optional services except for the few we need.
    if ((UBI7_COMPAT)); then
        #mask systemd-machine-id-commit.service - partial fix for https://bugzilla.redhat.com/show_bug.cgi?id=1472439
        systemctl mask systemd-remount-fs.service dev-hugepages.mount sys-fs-fuse-connections.mount systemd-logind.service getty.target console-getty.service systemd-udev-trigger.service systemd-udevd.service systemd-random-seed.service systemd-machine-id-commit.service
    else
        find /etc/systemd/system \
            /lib/systemd/system \
            -path '*.wants/*' \
            -not -name '*journald*' \
            -not -name '*systemd-tmpfiles*' \
            -not -name '*systemd-user-sessions*' \
            -not -name '*ssh*' \
            -not -name 'dbus.*' \
            -not -name 'polkit.*' \
            -not -name 'systemd-logind.*' \
            -not -name '*crond*' \
            -exec rm -fv {} \;
        systemctl set-default multi-user.target
    fi
    mkdir -p /var/log/journal
    systemd-tmpfiles --create --prefix /var/log/journal
    sed -r -i 's/^#?Storage\=.*/Storage=persistent/' /etc/systemd/journald.conf
    mkdir -p /etc/selinux/targeted/contexts
    echo '<busconfig><selinux></selinux></busconfig>' >/etc/selinux/targeted/contexts/dbus_contexts
}

fix_pam() {
    sed -r -i 's/Defaults\s+requiretty/Defaults\t!requiretty/g' /etc/sudoers
    if ((FIX_PAM)); then
        sed --follow-symlinks -i -r 's@^(session\s+)([a-z]+)(\s+pam_limits.so)@#&@' /etc/pam.d/* || true
        sed --follow-symlinks -i -r 's@^(session\s+)([a-z]+)(\s+system-auth)@#&@' /etc/pam.d/su* /etc/pam.d/systemd-user || true
        if test -e /etc/pam.d/sshd; then
            sed --follow-symlinks -i -r 's@^(session|account)(\s+)([a-z]+)(\s+)pam_(loginuid|nologin).so@#&@' /etc/pam.d/sshd
        fi
    fi
}

fix_system() {
    if [ "$(id -u)" != 0 ]; then
        echo >&2 "ERROR: Must be root to run fixsystemd"
        return 1
    fi
    FIX_PAM=${FIX_PAM:-1}
    FIX_SYSTEMD=${FIX_SYSTEMD:-1}
    while [ $# -gt 0 ]; do
        local cmd="$1"
        shift
        case "$cmd" in
            --no-fix-systemd) FIX_SYSTEMD=0;;
            --no-fix-pam) FIX_PAM=0;;
        esac
    done
    if ((FIX_SYSTEMD)); then
        fix_systemd
    fi

    if ((FIX_PAM)); then
        fix_pam
    fi
}

if [ "$(basename -- "$0")" == "$(basename -- "${BASH_SOURCE[0]}")" ]; then
    if ((XTRACE)); then
        set -x
    fi
    if [ "$1" == fixups ]; then
        fix_system "$@"
    fi
fi
