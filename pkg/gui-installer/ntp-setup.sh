#!/bin/bash

if [ `id -u` -ne 0 ]; then
        exec sudo "$0" "$@"
fi

export LC_ALL=C
export PATH=/usr/local/sbin:/usr/local/bin:/sbin:/usr/sbin:/usr/bin:/root/bin:/bin

ntp_addservers () {
    local server_list="$1"
    local iburst="$2"
    sed -i.orig '/^server/d' /etc/ntp.conf

    oldIFS=$IFS
    IFS=",$oldIFS"

    for server in $server_list; do
        if [ -n "$iburst" ] && [ "$iburst" == "iburst" ]; then
            echo "server $server iburst" >> /etc/ntp.conf
        else
            echo "server $server" >> /etc/ntp.conf
        fi
    done

    IFS=$oldIFS
}

if test -f /etc/redhat-release; then
    ELVERSION="$(grep -Eow '([0-9\.]+)' /etc/redhat-release | cut -f1 -d.)"
    ntp_addservers "$@"

    case "$ELVERSION" in
        6)
            chkconfig ntpd on
            service ntpd restart
            ;;
        7)
            systemctl enable ntpd
            systemctl start ntpd
            ;;
    esac
else
    DEBIAN_FRONTEND=noninteractive apt-get install -yqq ntp
    ntp_addservers "$@"

    service ntp restart
fi
rc=$?
exit $rc
