#!/bin/bash
# vim: ts=4 sts=4 et
#
# Builds optclang for EL6/7, AmazonLinux, Ubuntu14/16

set -e

ITERATION="${ITERATION:-13}"
VER="${1:-5.0.2}"
VMAJ="${VER:0:1}"
V="${VER%%\.[0-9]}"
NAME=clang${VMAJ}
INSTALL_PREFIX=/opt/clang${VMAJ}
DESC="A C language family front-end for LLVM"
LICENSE="NCSA"
URL="http://llvm.org/"
CATEGORY="Development/Languages"
LLDB=true

llvm_setup_trunk() {
    VER=trunk
    SRCDIR=$(pwd)/llvm-$VER
    rm -rf $SRCDIR
    # llvm from svn
    svn co $* http://llvm.org/svn/llvm-project/llvm/trunk $SRCDIR
    # clang
    cd $SRCDIR/tools
    svn co $* http://llvm.org/svn/llvm-project/cfe/trunk clang
    cd -
    # extra
    cd $SRCDIR/tools/clang/tools
    svn co $* http://llvm.org/svn/llvm-project/clang-tools-extra/trunk extra
    cd -
    # compiler-rt
    cd $SRCDIR/projects
    svn co $* http://llvm.org/svn/llvm-project/compiler-rt/trunk compiler-rt
    cd -
}

llvm_setup_src() {
    SRCDIR=$(pwd)/llvm-$VER
    if test -e "$SRCDIR"; then
        return
    fi
    for proj in lldb llvm cfe compiler-rt libcxx libcxxabi libunwind clang-tools-extra; do
        test -f ${proj}-${VER}.src.tar.xz || curl -fsSLO http://llvm.org/releases/${VER}/${proj}-${VER}.src.tar.xz
    done
    local srcdir=${SRCDIR}.$$
    mkdir -p ${srcdir}
    tar Jxf llvm-${VER}.src.tar.xz --strip-components=1 -C $srcdir

    mkdir -p $srcdir/tools/clang
    tar Jxf cfe-${VER}.src.tar.xz --strip-components=1 -C $srcdir/tools/clang

    mkdir -p $srcdir/tools/lldb
    tar Jxf lldb-${VER}.src.tar.xz --strip-components=1 -C $srcdir/tools/lldb
    mkdir -p $srcdir/tools/clang/tools/extra
    tar Jxf clang-tools-extra-${VER}.src.tar.xz --strip-components=1 -C $srcdir/tools/clang/tools/extra
    mkdir -p $srcdir/projects/compiler-rt
    tar Jxf compiler-rt-${VER}.src.tar.xz --strip-components=1 -C $srcdir/projects/compiler-rt
    mkdir -p $srcdir/projects/libcxx
    tar Jxf libcxx-${VER}.src.tar.xz --strip-components=1 -C $srcdir/projects/libcxx
    mkdir -p $srcdir/projects/libcxxabi
    tar Jxf libcxxabi-${VER}.src.tar.xz --strip-components=1 -C $srcdir/projects/libcxxabi
    mkdir -p $srcdir/projects/libunwind
    tar Jxf libunwind-${VER}.src.tar.xz --strip-components=1 -C $srcdir/projects/libunwind
    rm -rf $SRCDIR
    mv $srcdir $SRCDIR
}

llvm_build() {
    TMPDIR=/tmp/$(id -u)/clang-$VER
    DESTDIR=$TMPDIR/rootfs
    BUILDDIR=$TMPDIR/build
    if command -v clang++ >/dev/null && command -v clang++ >/dev/null; then
        CC="${CC:-clang}"
        CXX="${CXX:-clang++}"
    else
        CC="${CC:-gcc}"
        CXX="${CXX:-g++}"
    fi

    if $CC --version | grep -q ^gcc; then
        if test -e /opt/rh/devtoolset-7/enable; then
            if ! scl_enabled devtoolset-7; then
                . /opt/rh/devtoolset-7/enable
            fi
        fi
        HOST_GCC="$(cd $(dirname $(readlink -f $(command -v $CC)))/.. && pwd)"
        CM_DEFINES="-DCMAKE_C_COMPILER=$CC -DCMAKE_CXX_COMPILER=$CXX -DGCC_INSTALL_PREFIX=${HOST_GCC} -DCMAKE_CXX_LINK_FLAGS=\"-L${HOST_GCC}/lib64 -Wl,-rpath,${HOST_GCC}/lib64\""
    else
        CM_DEFINES="-DCMAKE_C_COMPILER=$CC -DCMAKE_CXX_COMPILER=$CXX"
    fi

    if command -v cmake3 &>/dev/null; then
        CMAKE=cmake3
    else
        CMAKE=cmake
    fi

    CM_DEFINES="$CM_DEFINES -DCLANG_BUILD_EXAMPLES=false -DCLANG_INCLUDE_DOCS=false -DCLANG_INCLUDE_TESTS=false -DCLANG_INCLUDE_DOCS=false -DCLANG_BUILD_EXAMPLES=false -DLLVM_INCLUDE_EXAMPLES=false -DLLVM_INCLUDE_DOCS=false -DLLVM_INCLUDE_TESTS=false -DLLVM_ENABLE_LIBCXX=true -DCMAKE_LIBRARY_PATH=/opt/clang${VMAJ}/lib" # -DCMAKE_INCLUDE_PATH=/usr/include/c++/6.4.1/x86_64-amazon-linux"
    if [ -d /usr/include/c++/6.4.1/x86_64-amazon-linux ]; then
        CM_DEFINES="$CM_DEFINES -DCMAKE_INCLUDE_PATH=/usr/include/c++/6.4.1/x86_64-amazon-linux"
    fi

    rm -rf ${TMPDIR} ${BUILDDIR}
    mkdir -p $DESTDIR/etc/ld.so.conf.d $DESTDIR/${INSTALL_PREFIX} $BUILDDIR
    OSID=$(osid)
    case "$OSID" in
        rhel7 | el7) sudo yum install -y libedit-devel libxml2-devel ncurses-devel python-devel swig ccache ;;
        rhel6 | el6)
            rpm -q ius-release || sudo yum install -y https://centos6.iuscommunity.org/ius-release.rpm
            sudo yum install -y python27-devel python27-pip libedit-devel libxml2-devel ncurses-devel python-devel swig ccache
            ;;
        amzn1)
            # A bug in the amzn1 optclang5 package named the conf file poorly and this causes
            # optclang6 build to fail
            if test -e /etc/ld.so.conf.d/Amazon; then
                sudo mv /etc/ld.so.conf.d/{Amazon,optclang5.conf}
            fi
            sudo ldconfig
            sudo yum install -y epel-release
            rpm -q ius-release || sudo yum install -y https://centos6.iuscommunity.org/ius-release.rpm
            sudo yum install --enablerepo='xcalar-deps*' --enablerepo=epel --enablerepo=ius -y python27-devel python27-pip libedit-devel libxml2-devel ncurses-devel python-devel swig ccache optcmake3
            ;;
        ub*)
            sudo apt-get update -qq
            sudo apt-get purge -y libeditline-dev || true
            sudo DEBIAN_FRONTEND=noninteractive apt-get install -y build-essential subversion swig python2.7-dev libedit-dev libncurses5-dev ccache libeditline-dev ccache
            ;;
        *)
            echo >&2 "Unknown OS: $OS"
            exit 1
            ;;
    esac

    # Setup cmake with ccache
    test -e $SRCDIR/CMakeLists.txt.orig || cp $SRCDIR/CMakeLists.txt $SRCDIR/CMakeLists.txt.orig
    (
        cat $SRCDIR/CMakeLists.txt.orig
        cat <<-EOF
# Configure CCache if available
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)
EOF
    ) >$SRCDIR/CMakeLists.txt
    export CCACHE_BASEDIR=$SRCDIR
    cd $BUILDDIR
    $CMAKE $CM_DEFINES \
        -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
        -DLLVM_ENABLE_ASSERTIONS=OFF \
        -DCMAKE_BUILD_TYPE="MinSizeRel" \
        -DLLVM_TARGETS_TO_BUILD="X86" \
        $SRCDIR -G Ninja
    DESTDIR=$DESTDIR ninja
    test -e lib64/python2.7 && cp -a lib64/python2.7 lib/ || true
    DESTDIR=$DESTDIR ninja install
    # install symlink clang++-${VER} pointing to clang++
    ln -sfn clang++ ${DESTDIR}/${INSTALL_PREFIX}/bin/clang++-${V}

    # Add /opt/clang/lib to LD_LIBRARY_PATH
    echo "$INSTALL_PREFIX/lib" | tee ${DESTDIR}/etc/ld.so.conf.d/${NAME}.conf
    printf '#!/bin/sh\nldconfig' >${DESTDIR}/ldconfig.sh
    chmod +x ${DESTDIR}/ldconfig.sh
}

llvm_pkg() {
    set +e
    if [ "$VER" = "4.0.0" ]; then
        cp -p $SRCDIR/tools/clang/tools/scan-build/bin/scan-build ${DESTDIR}/${INSTALL_PREFIX}/bin
        cp -p $SRCDIR/tools/clang/tools/scan-build/libexec/ccc-analyzer ${DESTDIR}/${INSTALL_PREFIX}/bin
        cp -p $SRCDIR/tools/clang/tools/scan-build/libexec/c++-analyzer ${DESTDIR}/${INSTALL_PREFIX}/bin
        cp -p $SRCDIR/tools/clang/tools/scan-build/share/scan-build/sorttable.js ${DESTDIR}/${INSTALL_PREFIX}/bin
        cp -p $SRCDIR/tools/clang/tools/scan-build/share/scan-build/scanview.css ${DESTDIR}/${INSTALL_PREFIX}/bin
        cp -rp $SRCDIR/tools/clang/tools/scan-view/* ${DESTDIR}/${INSTALL_PREFIX}/bin
    fi
    cd -
    set -e
    OSID="$(bash osid)"
    case "$OSID" in
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
        ub*)
            fmt=deb
            pkg_install="dpkg -i"
            dist=$OSID
            ITERATION="${ITERATION}"
            #FPMEXTRA+="-d libbsd0 -d libc6 -d libedit2 -d libgcc1 -d libicu55 -d liblzma5 -d libncurses5 -d libstdc++6 -d libtinfo5 -d libxml2 -d zlib1g "
            ;;
        amzn*)
            fmt=rpm
            pkg_install="yum localinstall -y"
            dist=$OSID
            ;;
        *)
            echo >&2 "Unknown OS: $OSID"
            ;;
    esac
    if [ "$fmt" = rpm ]; then
        FPMEXTRA+="--rpm-dist $dist --rpm-autoreqprov"
    fi
    if command -v fpm &>/dev/null; then
        # Use fpm to build a native .rpm or .deb package called $NAME
        fpm -s dir -t $fmt --name opt${NAME} -v $VER --iteration $ITERATION \
            --after-remove $DESTDIR/ldconfig.sh --after-install $DESTDIR/ldconfig.sh \
            --url "$URL" --license "$LICENSE" --description "$DESC" --category "$CATEGORY" \
            $FPMEXTRA -f -C $DESTDIR \
            "${INSTALL_PREFIX#/}" etc
    else
        # if fpm isn't around, then copy $DESTDIR to /
        tar czf ${NAME}-${VER}-${ITERATION}.${OSID}.tar.gz -C $DESTDIR "${INSTALL_PREFIX#/}" etc
    fi
}

llvm_setup_src ${1}
llvm_build
llvm_pkg
exit
