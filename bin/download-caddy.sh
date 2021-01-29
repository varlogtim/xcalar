#!/bin/bash
#
# download-caddy.sh - Downloads the currently blessed version of caddy
# and attempts to setcap on the executable so it can bind port < 1024
# as non-root

CADDY_URL="${CADDY_URL:-http://repo.xcalar.net/deps/caddy_0.11.0-103_linux_amd64.tar.gz}"

set -e
[ $# -gt 0 ] || set -- caddy
case "$CADDY_URL" in
    *.tar.gz) curl -fsSL "$CADDY_URL" | tar zxf - -O caddy > "${1}.$$" ;;
    *.gz) curl -fsSL "$CADDY_URL" | gzip -dc > "${1}.$$" ;;
    *) curl -fsSL "$CADDY_URL" > "${1}.$$";;
esac

chmod +x "${1}.$$"
if ! sudo -n setcap cap_net_bind_service=+ep "${1}.$$"; then
    echo >&2 "WARNING: Unable to setcap cap_net_bind_service=+ep ${1}"
fi
mv "${1}.$$" "$1"
