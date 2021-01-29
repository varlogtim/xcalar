#!/bin/bash
#
# shellcheck disable=SC1091
set -ex

if ((JENKINS_HARNESS)); then
    :
else
    export XLRDIR=$PWD
    export XLRGUIDIR=$PWD/xcalar-gui
    export XLRINFRADIR=$PWD/xcalar-infra

    source doc/env/xc_aliases
    source bin/xcsetenv
fi

make -C docker

rm -fv $XLRDIR/xcalar-testdata*.{rpm,deb,tar.gz} $XLRDIR/build-my-rpm.sh

# Generate a local script instead of trying to string
# together a bunch of crun commands to do the right
# thing
cat <<EOF >$XLRDIR/build-my-rpm.sh
#!/bin/bash

set -e
set -o pipefail

rm -rf \$HOME/rpmbuild/
rpmdev-setuptree
cat > \$HOME/.rpmmacros <<XEOF
%global debug_package %{nil}
%__arch_install_post %{nil}
%__os_install_post %{nil}
%global prefix /opt/xcalar

# disable automatic dependency and provides generation with:
%__find_provides %{nil}
%__find_requires %{nil}
%__check_files  %{nil}

%_use_internal_dependency_generator 0
%_unpackaged_files_terminate_build 0

%_xlrdir $XLRDIR
%_version $(cat $XLRDIR/VERSION)
%_build_number ${BUILD_NUMBER:-1}

Autoprov: 0
Autoreq: 0
XEOF

rpmbuild --nocheck -bb pkg/rpm/xcalar-testdata.spec
find \$HOME/rpmbuild/RPMS/ -name '*.rpm' | xargs -r -n1 -I{} cp -v {} $XLRDIR/
EOF

crun el7-build /bin/bash -x $XLRDIR/build-my-rpm.sh
