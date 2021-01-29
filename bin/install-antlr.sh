#!/bin/bash

set -e

NAME=antlr
VERSION=${VERSION:-4.5.3}
PKGNAME=xcalar-${NAME}
BUILD_NUMBER=${BUILD_NUMBER:-4}
JAR=${NAME}-${VERSION}-complete.jar
SRCURL=http://repo.xcalar.net/deps/${JAR}
PREFIX=/opt/xcalar
DEST=${PREFIX}/lib/${JAR}
LICENSE=BSD
URL=http://antlr.org

TMPDIR=$(mktemp -d -t ${NAME}.XXXX)

mkdir -p ${TMPDIR}$(dirname $DEST) ${TMPDIR}${PREFIX}/bin/

curl -f "$SRCURL" -o ${TMPDIR}${DEST}
cat > ${TMPDIR}${PREFIX}/bin/${NAME} <<EOF
#!/bin/sh

CLASSPATH="\$CLASSPATH:$DEST"
export CLASSPATH
java org.antlr.v4.Tool "\$@"
EOF
chmod +x ${TMPDIR}${PREFIX}/bin/${NAME}

FPMCOMMON=(--version $VERSION --iteration $BUILD_NUMBER \
--url "$URL" --description 'ANother Tool for Language Recognition' \
--license "${LICENSE}" -f --prefix=${PREFIX} -C ${TMPDIR}${PREFIX})

fpm -s dir -t rpm -a noarch --name ${PKGNAME} "${FPMCOMMON[@]}"
fpm -s dir -t deb -a all --name ${PKGNAME} "${FPMCOMMON[@]}"
rm -rf $TMPDIR
