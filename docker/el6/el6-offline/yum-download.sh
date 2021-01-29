#!/bin/bash
set -e

pkg_deps () {
    yum deplist "$@" | grep provider | sed -Ee 's/^\s*//g' | cut -d' ' -f2 | sort | uniq  | grep -v i.86
}

pkg_system_deps () {
    yum deplist "$@" -x boost\* -x nodejs\* -x protobuf10\* -x xcalar-node10\* -x xcalar-python36\* -x xcalar-sqldf\* -x xcalar-antlr\* -x xcalar-arrow-libs\* -x xcalar.\* | grep provider | sed -Ee 's/^\s*//g' | cut -d' ' -f2 | sort | uniq  | grep -v i.86 | tr '\n' ' ' | tr '\r' ' '
}

ELVERSION="$(grep -Eow '([0-9\.]+)' /etc/redhat-release | cut -d'.' -f1)"
ELV="el${ELVERSION}"
TMP="$(mktemp -d --tmpdir rpm.XXXXXX)"
D="${TMP}/${ELV}"
mkdir -p "${D}"
yumdownloader --disablerepo='*' --enablerepo='epel' --enablerepo='xcalar-deps' --enablerepo='xcalar-deps-common' --destdir=${D} $(pkg_deps "$@")
rm -f ${D}/*.i?86.rpm
echo "Xcalar dependencies:" > build/xcalar-dependencies-${ELV}.txt
echo "" >> build/xcalar-dependencies-${ELV}.txt
ALL_SYSTEM_DEPS="$(pkg_system_deps "$@" "xcalar-python36")"
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
        *jvm*|*jdk*)
            test -z "$JAVA_DEPS" && JAVA_DEPS="$dep" || JAVA_DEPS="$JAVA_DEPS $dep"
            ;;
        *)
            test -z "$SYSTEM_DEPS" && SYSTEM_DEPS="$dep" || SYSTEM_DEPS="$SYSTEM_DEPS $dep"
            ;;
    esac
done
echo -n "$SYSTEM_DEPS" > build/${ELV}-config/system-rpm-deps.${ELV}.txt
chown $(stat --format '%u:%g' `pwd`) build/${ELV}-config/system-rpm-deps.${ELV}.txt
echo -n "$KRB_DEPS" > build/${ELV}-config/krb-rpm-deps.${ELV}.txt
chown $(stat --format '%u:%g' `pwd`) build/${ELV}-config/krb-rpm-deps.${ELV}.txt
echo -n "$JAVA_DEPS" > build/${ELV}-config/java-rpm-deps.${ELV}.txt
chown $(stat --format '%u:%g' `pwd`) build/${ELV}-config/java-rpm-deps.${ELV}.txt
tar cf ${ELV}.tar -C ${TMP} ${ELV}
chown $(stat --format '%u:%g' `pwd`) ${ELV}.tar
rm -rf $TMP
