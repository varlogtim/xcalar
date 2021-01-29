#!/bin/bash
#
# Build a meta-package that (mostly) just depends
# on other packages. It adds a necessary symlink to
# one of the oracle libraries and add the ORACLE_LIBS
# dir to ld path

set -e

NAME=xcalar-odbcdeps
VERSION=${VERSION:-1.0}
ITERATION=${ITERATION:-6}

ORACLE_LIBS=/usr/lib/oracle/12.2/client64/lib

TMPDIR=$(mktemp -d -t xcalar-odbcdeps.XXXXXX)

mkdir -p $TMPDIR/etc/ld.so.conf.d/ $TMPDIR${ORACLE_LIBS}
ln -s libclntsh.so.12.1 ${TMPDIR}$ORACLE_LIBS/libclntsh.so
echo $ORACLE_LIBS > ${TMPDIR}/etc/ld.so.conf.d/${NAME}.conf

cat > $TMPDIR/after-install.sh <<'EOF'
#!/bin/bash
ldconfig
EOF

cat > $TMPDIR/after-remove.sh <<'EOF'
#!/bin/bash
ldconfig
EOF

fpm -s dir -t rpm --name ${NAME} \
    --version $VERSION --iteration $ITERATION \
    -d oracle-instantclient12.2-basic \
    -d oracle-instantclient12.2-odbc \
    -d msodbcsql \
    -d msodbcsql17 \
    -d mssql-tools \
    -d 'unixODBC >= 2.3.1' \
    -d freetds \
    -d mysql-libs \
    -d mysql-connector-odbc \
    -d postgresql-odbc \
    -d postgresql-libs \
    --vendor 'Xcalar, Inc.' \
    --license Proprietary \
    --description 'Meta-package to pull in all MS/Oracle SQL and ODBC dependencies' \
    --url 'https://xcalar.com' \
    --after-install $TMPDIR/after-install.sh \
    --after-remove $TMPDIR/after-remove.sh \
    -f -C $TMPDIR \
    usr etc

rm -rf $TMPDIR
