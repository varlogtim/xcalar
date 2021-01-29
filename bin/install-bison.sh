#!/bin/bash
NAME=bison
VER=2.5.1
ITERATION=${ITERATION:-5}
TMPDIR=/tmp/`id -un`/bison
DESTDIR=$TMPDIR/rootfs

rm -rf ${DESTDIR}
mkdir -p ${DESTDIR}
rm -rf ${NAME}-${VER}
curl -sSL http://ftp.gnu.org/gnu/${NAME}/${NAME}-${VER}.tar.gz | tar zxf -
pushd ${NAME}-${VER}
./configure --prefix=/usr
make -j`nproc`
make DESTDIR=$DESTDIR install
rm -rf $DESTDIR/usr/share/info/dir
mv $DESTDIR/usr/bin/{yacc,bison.yacc}
mv $DESTDIR/usr/share/man/man1/{yacc.1,bison.yacc.1}
popd
cat > "$TMPDIR/after-install.sh"<<EOF
#!/bin/sh
test -e /usr/bin/yacc || ln -sfn bison.yacc /usr/bin/yacc
EOF
chmod +x "$TMPDIR/after-install.sh"

cat > "$TMPDIR/after-remove.sh"<<EOF
#!/bin/sh
set +ex
yacc="$(readlink /usr/bin/yacc 2>/dev/null)"
if [ "$yacc" = "bison.yacc" ]; then
    rm -f /usr/bin/yacc
fi
EOF
chmod +x "$TMPDIR/after-remove.sh"

osid="$(bash osid)"
case "$osid" in
	*el6)
		fmt=rpm
		dist=el6
		pkg_install="yum localinstall -y"
		;;
	*el7)
		fmt=rpm
		dist=el7
		pkg_install="yum localinstall -y"
		;;
	ub14)
		fmt=deb
		pkg_install="dpkg -i"
		;;
	*)
		echo >&2 "Unknown OS: $osid"
		;;
esac
if [ "$fmt" = rpm ]; then
	FPMEXTRA="--rpm-dist $dist --rpm-autoreqprov"
fi

if command -v fpm &>/dev/null; then
	# Use fpm to build a native .rpm or .deb package called optclang
	fpm -s dir -t $fmt --name $NAME -v $VER --iteration $ITERATION --license 'GPLv3+' \
        --url 'http://www.gnu.org/software/bison/' --description 'A GNU general-purpose parser generator.' \
        --vendor 'Xcalar, Inc.'  --maintainer 'Xcalar BuildSystem <http://bugs.xcalar.com>' \
	--after-install $TMPDIR/after-install.sh --after-remove $TMPDIR/after-remove.sh $FPMEXTRA -f -C $DESTDIR usr
	sudo $pkg_install ${NAME}-${VER}*.${fmt}
else
	# if fpm isn't around, then copy $DESTDIR to /
	tar cf - -C $DESTDIR usr | tar xf - -C /
fi

rm -rf "${TMPDIR}"
