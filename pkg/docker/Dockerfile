FROM xcalar-el7-base:latest

ARG INSTALLER_URL
RUN set -o pipefail && curl -k -Lf $INSTALLER_URL -o /tmp/installer.sh && bash /tmp/installer.sh --nostart --caddy && rm -f /tmp/installer.sh
ADD 01-xcalar-attrs /etc/fix-attrs.d/
ADD defaultAdmin.json /etc/xcalar/
ADD cert.pem /etc/xcalar/
ADD cert.key /etc/xcalar/
ADD supervisor.conf /etc/xcalar/
RUN mkdir -m 0700 -p /var/opt/xcalar/config \
    && cp /etc/xcalar/defaultAdmin.json /var/opt/xcalar/config/ \
    && chmod 0600 /var/opt/xcalar/config/defaultAdmin.json \
    && chown -v -R xcalar:xcalar /var/opt/xcalar \
    && /opt/xcalar/scripts/genConfig.sh /etc/xcalar/template.cfg - localhost > /etc/xcalar/default.cfg \
    && chown -v xcalar:xcalar /etc/xcalar /etc/xcalar/default.cfg \
    && chown -v -R xcalar:xcalar /var/log/xcalar \
    && ls -al /var/opt/xcalar /var/opt/xcalar/config /etc/xcalar /var/log/xcalar \
    && mkdir -p /etc/services.d/sshd \
    && printf '#!/usr/bin/with-contenv sh\n/usr/sbin/sshd -D\n' > /etc/services.d/sshd/run \
    && chmod +x /etc/services.d/sshd/run \
    && mkdir -p /etc/services.d/rsyslogd \
    && printf '#!/usr/bin/with-contenv sh\n/usr/sbin/rsyslogd -n -c5\n' > /etc/services.d/rsyslogd/run \
    && chmod +x /etc/services.d/rsyslogd/run \
    && printf '#!/usr/bin/with-contenv sh\n/usr/bin/ssh-keygen -A -N ""\n' > /etc/cont-init.d/01-ssh-keygen.sh && chmod +x /etc/cont-init.d/01-ssh-keygen.sh \
    && curl -fL http://repo.xcalar.net/deps/supervisor-3.3.4-1.tar.gz | tar zxvf - -C /opt/xcalar/bin/ \
    && curl -fL http://repo.xcalar.net/deps/caddy-linux-amd64_0.10.12-1.tar.gz | tar zxvf - -C /opt/xcalar/bin \
    && setcap cap_net_bind_service=+ep /opt/xcalar/bin/caddy \
    && chown -v xcalar:xcalar /etc/xcalar/cert.* \
    && chmod 0400 /etc/xcalar/cert.key \
    && sed -i '/=root$/d' /etc/xcalar/supervisor.conf \
    && sed -i 's@tls.*@tls /etc/xcalar/cert.pem /etc/xcalar/cert.key@g' /etc/xcalar/Caddyfile

ENV XLRDIR=/opt/xcalar PATH=/opt/xcalar/bin:$PATH XCE_WORKDIR=/var/tmp/xcalar-root XCE_CADDYFILE=/etc/xcalar/Caddyfile XCE_HOME=/var/opt/xcalar \
    XCE_USER=xcalar XCE_CONFIG=/etc/xcalar/default.cfg XCE_LOGDIR=/var/log/xcalar XLRGUIDIR=/var/www/xcalar-gui XCE_NODEID=0

EXPOSE 22 8443 5000 8000 8000/udp

VOLUME ["/var/opt/xcalar"]

ENTRYPOINT ["/init"]
CMD ["/usr/bin/gosu", "xcalar:xcalar", "/opt/xcalar/bin/supervisord", "-n", "-c", "/etc/xcalar/supervisor.conf"]
