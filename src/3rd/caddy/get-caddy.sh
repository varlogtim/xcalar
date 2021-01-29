#!/bin/bash

# Acquired via:
## wget 'https://caddyserver.com/download/build?os=linux&arch=amd64&features=cors%2Cfilemanager%2Cgit%2Chugo%2Cipfilter%2Cjwt%2Clocale%2Cmailout%2Cminify%2Cprometheus%2Cratelimit%2Crealip%2Csearch%2Cupload'


DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
GOOS="${GOOS:-$(uname -s | tr A-Z a-z)}"
VER=${VER:-0.9.1}
CADDY="caddy-${GOOS}-amd64_${VER}"

get_caddy () {
    (
    set -x
    wget 'https://caddyserver.com/download/build?os='$GOOS'&arch=amd64&features=cors%2Cfilemanager%2Cgit%2Chugo%2Cipfilter%2Cjwt%2Clocale%2Cmailout%2Cminify%2Cprometheus%2Cratelimit%2Crealip%2Csearch%2Cupload%2Cgooglecloud%2Croute53'
    )
}

set -e
curl -sSL -O http://repo.xcalar.net/deps/$CADDY
md5sum -c MD5SUMS.txt
mv $CADDY caddy
chmod +x caddy
