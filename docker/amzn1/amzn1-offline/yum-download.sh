#!/bin/bash
set -e

set_pkg_version() {
    while read dep; do
        case "$dep" in
            xcalar-python36*) echo "xcalar-python36-${XCALAR_PYTHON_VERSION} " ;;
            *) echo "$dep " ;;
        esac
    done
}

pkg_deps () {
    yum deplist "$@" | grep provider | sed -Ee 's/^\s*//g' | cut -d' ' -f2 | sort | uniq  | grep -v i.86 | sed -e 's/\.x86_64//g' | set_pkg_version || true
}

pkg_system_deps () {
    yum deplist "$@" -x boost\* -x nodejs\* -x pytz\* -x supervisor\* -x protobuf10\* -x xcalar-jre8\* -x xcalar-node10\* -x xcalar-python36\* -x xcalar-sqldf\* -x xcalar-antlr\* -x xcalar-arrow-libs\* -x xcalar.\* | grep provider | sed -Ee 's/^\s*//g' | cut -d' ' -f2 | sort | uniq  | grep -v i.86 | tr '\n' ' ' | tr '\r' ' ' || true
}

if ! source /etc/os-release 2>/dev/null; then
    ELVERSION=6
    ELV=el6
else
    case "$VERSION_ID" in
        2018*) ELVERSION=1; ELV=amzn1;;
        2) ELVERSION=2; ELV=amzn2;;
        7*) ELVERSION=7; ELV=el7;;
    esac
fi
TMP="$(mktemp -d --tmpdir rpm.XXXXXX)"
D="${TMP}/${ELV}"
mkdir -p "${D}"
yumdownloader --resolve --disablerepo='*' --disableplugin=priorities --enablerepo='*epel' --enablerepo='xcalar-deps' --enablerepo='xcalar-deps-common' --destdir=${D} $(pkg_deps "$@" xcalar-odbcdeps)
rm -f ${D}/*.i?86.rpm
echo "Xcalar dependencies:" > build/xcalar-dependencies-${ELV}.txt
echo "" >> build/xcalar-dependencies-${ELV}.txt
ALL_SYSTEM_DEPS="$(pkg_system_deps "$@" "xcalar-python36" "xcalar-jre8")"
echo "$ALL_SYSTEM_DEPS" >> build/xcalar-dependencies-${ELV}.txt
echo "" >> build/xcalar-dependencies-${ELV}.txt
echo "Xcalar built packages:" >> build/xcalar-dependencies-${ELV}.txt
echo "" >> build/xcalar-dependencies-${ELV}.txt
ls -1 ${D} >> build/xcalar-dependencies-${ELV}.txt
chown $(stat --format '%u:%g' `pwd`) build/xcalar-dependencies-${ELV}.txt
for dep in $ALL_SYSTEM_DEPS; do
    case "$dep" in
        *krb*|*sasl*)
            test -z "$KRB_DEPS" && KRB_DEPS="$dep" || KRB_DEPS="$KRB_DEPS $dep"
            ;;
        *jre*|*jvm*|*jdk*)
            test -z "$JAVA_DEPS" && JAVA_DEPS="$dep" || JAVA_DEPS="$JAVA_DEPS $dep"
            ;;
        *)
            test -z "$SYSTEM_DEPS" && SYSTEM_DEPS="$dep" || SYSTEM_DEPS="$SYSTEM_DEPS $dep"
            ;;
    esac
done
test -d build/${ELV}-config || mkdir -p build/${ELV}-config
chown $(stat --format '%u:%g' `pwd`) build/${ELV}-config
echo -n "$SYSTEM_DEPS" > build/${ELV}-config/system-rpm-deps.${ELV}.txt
chown $(stat --format '%u:%g' `pwd`) build/${ELV}-config/system-rpm-deps.${ELV}.txt
echo -n "$KRB_DEPS" > build/${ELV}-config/krb-rpm-deps.${ELV}.txt
chown $(stat --format '%u:%g' `pwd`) build/${ELV}-config/krb-rpm-deps.${ELV}.txt
echo -n "$JAVA_DEPS" > build/${ELV}-config/java-rpm-deps.${ELV}.txt
chown $(stat --format '%u:%g' `pwd`) build/${ELV}-config/java-rpm-deps.${ELV}.txt
tar cf ${ELV}.tar -C ${TMP} ${ELV}
chown $(stat --format '%u:%g' `pwd`) ${ELV}.tar
rm -rf $TMP
