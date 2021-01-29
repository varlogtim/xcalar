#!/bin/bash

if [ "$1" = /usr/sbin/init ]; then
    exec "$@"
    exit 1
fi

ssh-host-keys.sh

case "$(osid)" in
    ub14)
    service rsyslog restart
    service ssh start
    ;;
    el6)
    service rsyslog restart
    service sshd start
    ;;
    el7)
    systemctl restart rsyslog
    systemctl start sshd
    ;;
    *)
    echo >&2 "Uknown os: $(osid)"
    exit 1
    ;;
esac
/usr/sbin/sshd -f /etc/ssh/sshd-second_config
exec "$@"
