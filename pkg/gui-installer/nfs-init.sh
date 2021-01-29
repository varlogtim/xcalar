#!/bin/bash

if [ `id -u` -ne 0 ]; then
	exec sudo "$0" "$@"
fi

export LC_ALL=C
export PATH=/usr/local/sbin:/usr/local/bin:/sbin:/usr/sbin:/usr/bin:/root/bin:/bin

mkdir -p /srv/xcalar
echo "/srv/xcalar *(rw,sync,no_subtree_check,fsid=0,no_root_squash)" > /etc/exports

iptables_addport () {
    local port="$1"
    local proto="$2"

    if ! iptables -C INPUT -m state --state NEW -m $proto -p $proto --dport $port -j ACCEPT &>/dev/null; then
        iptables -I INPUT 2 -m state --state NEW -m $proto -p $proto --dport $port -j ACCEPT
    fi
}

if test -f /etc/redhat-release; then
    ELVERSION="$(grep -Eow '([0-9\.]+)' /etc/redhat-release | cut -f1 -d.)"
    sed -i -E -e 's/^#([A-Z_]+)_PORT=/\1_PORT=/g' \
            -e 's/^#([A-Z_]+)_UDPPORT=/\1_UDPPORT=/g' \
            -e 's/^#([A-Z_]+)_TCPPORT=/\1_TCPPORT=/g' \
            /etc/sysconfig/nfs

    . /etc/sysconfig/nfs

    if command -v firewall-cmd &>/dev/null && firewall-cmd --state &>/dev/null; then
        firewall-cmd --permanent --add-port=2049/tcp
        for port in 111 $LOCKD_TCPPORT $LOCKD_UDPPORT $MOUNTD_PORT $RQUOTAD_PORT $STATD_PORT $STATD_OUTGOING_PORT; do
            firewall-cmd --permanent --add-port=$port/tcp
            firewall-cmd --permanent --add-port=$port/udp
        done
        firewall-cmd --reload
        iptables-save > /etc/sysconfig/iptables
    elif command -v iptables &>/dev/null; then
        if [ $(iptables-save | wc -l) -gt 0 ]; then
            iptables_addport 2049 tcp
            for port in 111 $LOCKD_TCPPORT $LOCKD_UDPPORT $MOUNTD_PORT $RQUOTAD_PORT $STATD_PORT $STATD_OUTGOING_PORT; do
                iptables_addport $port tcp
                iptables_addport $port udp
            done
            iptables-save > /etc/sysconfig/iptables
        fi
    fi

    case "$ELVERSION" in
        6)
            chkconfig rpcbind on
            chkconfig nfs on
            service rpcbind restart
            service nfs restart
            ;;
        7)
            systemctl enable rpcbind
            systemctl enable nfs-server
            systemctl restart nfs-config
            systemctl restart rpcbind
            systemctl restart nfs-server
            ;;
    esac
    exportfs -a
else
    DEBIAN_FRONTEND=noninteractive apt-get install -yqq nfs-kernel-server
    . /etc/default/nfs-kernel-server
    . /etc/default/nfs-common
    service nfs-kernel-server restart
    update-rc.d nfs-kernel-server defaults
fi
rc=$?
exit $rc


