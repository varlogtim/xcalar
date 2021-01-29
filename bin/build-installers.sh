#!/bin/bash
#
# # SUMARY:
# This used to be the inline code from BuildMaster/BuildTrunk.
# Runs build-rpms.sh, build-offline.sh and build-gui-installer-os.sh in containers to build
# all the necessary packages and related dependencies to produce the final installer

set -e

die () {
    local rc=$1
    shift
    test $rc -eq 0 && echo >&2 "Success: $*" || echo >&2 "** ERROR($rc): $*"
    exit $rc
}

safe_cp() {
    if [ -n "$1" ] && [ -r "$1" ]; then
        cp "$@"
    else
        echo "safe_cp: Skipping $1 -> $2"
    fi
}


if ! test -e "bin/crun"; then
    echo >&2 "Must be in XLRDIR source root"
    exit 1
fi
if ! test -n "$XLRDIR"; then
    echo >&2 "Must set XLRDIR"
    exit 1
fi

export BUILD_VERSION="$(cat $XLRDIR/VERSION | tr -d '\n')"
export TMPDIR="${TMPDIR:-/tmp/`id -u`}"
export XLRGUIDIR="${XLRGUIDIR:-$XLRDIR/xcalar-gui}"
export JOB_NAME="${JOB_NAME:-local}"
export BUILD_NUMBER="${BUILD_NUMBER:-1}"
export BUILD_DIRECTORY="${BUILD_DIRECTORY:-$TMPDIR/builds/$JOB_NAME}"
export XCE_GIT_BRANCH="${XCE_GIT_BRANCH:-trunk}"
export XD_GIT_BRANCH="${XD_GIT_BRANCH:-trunk}"
export XD_GIT_REPOSITORY="${XD_GIT_REPOSITORY:-ssh://gerrit.int.xcalar.com:29418/xcalar/xcalar-gui.git}"
export DISABLE_FUNC_TESTS="${DISABLE_FUNC_TESTS:-false}"
export GUI_PRODUCT="${GUI_PRODUCT:-XD}"
export BUILD_TYPES="${BUILD_TYPES:-prod}"
export PLATFORMS="${PLATFORMS:-el7 amzn2}"

DEF_SQLDF_VERSION="$(. "$XLRDIR"/src/3rd/spark/BUILD_ENV; echo $SQLDF_VERSION)"
export SQLDF_VERSION="${SQLDF_VERSION:-$DEF_SQLDF_VERSION}"

echo "BUILD_DEBUG: PLATFORMS = $PLATFORMS"
echo "BUILD_DEBUG: BUILD_TYPES = $BUILD_TYPES"
echo "BUILD_DEBUG: SQLDF_VERSION = $SQLDF_VERSION"
echo "BUILD_DEBUG: SQLDF_BUILD_NUMBER = $SQLDF_BUILD_NUMBER"

if [ "${IS_RC}" = true ]; then
    export RC=rc
fi

# Some defaults so users can run this
mkdir -p "$TMPDIR/$$" "$BUILD_DIRECTORY"
commitSha1="${GIT_SHA_XCE:-$(git rev-parse --verify HEAD | cut -c1-8)}"
export commitSha1
buildDir="$BUILD_DIRECTORY/../byJob/$JOB_NAME/$BUILD_NUMBER"
wrkDir="$XLRDIR/wrkDir/${JOB_NAME}$(date '+%s')"
mkdir -p "$TMPDIR/$$" "$BUILD_DIRECTORY" "$buildDir" "$BUILD_DIRECTORY/../byHash" "$BUILD_DIRECTORY/../byJob/${JOB_NAME}" "$wrkDir/debug" "$wrkDir/prod"

cwd=$(pwd)
cd "$XLRGUIDIR"
guiCommitSha1="${GIT_SHA_XD:-$(git rev-parse --short=8 --verify HEAD)}"
touch "$wrkDir/BUILD_SHA"
echo "XCE: $XCE_GIT_BRANCH ($commitSha1)" > "$wrkDir/BUILD_SHA"
echo "XD: $XD_GIT_BRANCH ($guiCommitSha1)" >> "$wrkDir/BUILD_SHA"
cd "$cwd"
cp "$wrkDir/BUILD_SHA" "$buildDir/BUILD_SHA"

cp "$buildDir/BUILD_SHA" .

# Build
for BUILD_TYPE in $BUILD_TYPES; do
    rm -rf ${XLRDIR}/build/*

    mkdir -p ${XLRDIR}/build/xce
    export BUILD_TYPE
    echo "BUILD_DEBUG: Building $BUILD_TYPE installer"
    make -f make/Installer.mk
    INSTALLER="${XLRDIR}/build/xcalar-${BUILD_VERSION}-${BUILD_NUMBER}-installer"
    if [ "$SKIP_TEST_INSTALLERS" = true ]; then
        echo >&2 "** SKIPPING TESTING OF $INSTALLER **"
    else
        echo >&2 "** TESTING $INSTALLER **"
        $XLRDIR/bin/installer/tests/test-installers.sh $INSTALLER
    fi
    INSTALLER_USER="${XLRDIR}/build/xcalar-${BUILD_VERSION}-${BUILD_NUMBER}-userinstaller"
    INSTALLER_GUI_USER="${XLRDIR}/build/xcalar-${BUILD_VERSION}-${BUILD_NUMBER}-gui-userinstaller"
    mkdir -p "$buildDir/${BUILD_TYPE}"
    for plat in $PLATFORMS; do
        CONFIG_TARBALL="$(find "$XLRDIR/build/${plat}-config" -name "configuration_tarball-*.${plat}.tar.gz")"
        safe_cp "$CONFIG_TARBALL" "$buildDir/${BUILD_TYPE}"
        safe_cp "$XLRDIR/build/xcalar-dependencies-${plat}.txt" "$buildDir/${BUILD_TYPE}"
    done

    safe_cp "$INSTALLER" "$buildDir/${BUILD_TYPE}"
    safe_cp "$INSTALLER_USER" "$buildDir/${BUILD_TYPE}"
    safe_cp "$INSTALLER_GUI_USER" "$buildDir/${BUILD_TYPE}"
    # these are consumed in $XLRDIR/build by the gui-installer Dockerfile
    safe_cp "$wrkDir/${BUILD_TYPE}/opt/xcalar/bin/licenseCheck" "$XLRDIR/build"
    safe_cp "$wrkDir/${BUILD_TYPE}/opt/xcalar/scripts/SetXceDefaults.sh" "$XLRDIR/build"
done

rm -rf "$wrkDir/*"

cd $BUILD_DIRECTORY
rm -f xcalar-latest
ln -sfn $buildDir "${XCE_GIT_BRANCH//\//-}"
ln -sfn $buildDir xcalar-latest

# refactor this code, maybe in the loop, to avoid explicitly calling out prod and debug

if mkdir -p $BUILD_DIRECTORY/../byJob/$JOB_NAME; then
   ln -sfn $BUILD_NUMBER $BUILD_DIRECTORY/../byJob/$JOB_NAME/lastSuccessful
   ln -sfn $BUILD_NUMBER $BUILD_DIRECTORY/../byJob/$JOB_NAME/lastSuccessful-$BUILD_VERSION
   cd  $BUILD_DIRECTORY/../byJob/$JOB_NAME/
   latest_prod_install="$BUILD_NUMBER/prod/xcalar-${BUILD_VERSION}-${BUILD_NUMBER}-installer"
   latest_prod_gui_userinstall="$BUILD_NUMBER/prod/xcalar-${BUILD_VERSION}-${BUILD_NUMBER}-gui-userinstaller"
   latest_prod_userinstall="$BUILD_NUMBER/prod/xcalar-${BUILD_VERSION}-${BUILD_NUMBER}-userinstaller"
   latest_debug_install="$BUILD_NUMBER/debug/xcalar-${BUILD_VERSION}-${BUILD_NUMBER}-installer"
   latest_debug_userinstall="$BUILD_NUMBER/debug/xcalar-${BUILD_VERSION}-${BUILD_NUMBER}-userinstaller"
   test -e "$latest_prod_install" && ln -sfn "$latest_prod_install" xcalar-latest-installer-prod
   test -e "$latest_prod_install" && ln -sfn "$latest_prod_install" xcalar-latest-$BUILD_VERSION-installer-prod
   test -e "$latest_debug_install" && ln -sfn "$latest_debug_install" xcalar-latest-installer-debug
   test -e "$latest_debug_install" && ln -sfn "$latest_debug_install" xcalar-latest-$BUILD_VERSION-installer-debug

   if [ -e "$latest_prod_gui_userinstall" ]; then
      ln -sfn "$latest_prod_gui_userinstall" xcalar-latest-gui-userinstaller-prod
      ln -sfn "$latest_prod_gui_userinstall" xcalar-latest-$BUILD_VERSION-gui-userinstaller-prod
   else
      echo "$latest_prod_gui_userinstall not found. This might be amzn only build"
   fi

   # check for node user installer, it won't be avilable for amzn build
   if [ -e "$latest_prod_userinstall" ]; then
      ln -sfn "$latest_prod_userinstall" xcalar-latest-userinstaller-prod
      ln -sfn "$latest_prod_userinstall" xcalar-latest-$BUILD_VERSION-userinstaller-prod
   else
      echo "$latest_prod_userinstall not found. This might be amzn only build"
   fi
   if [ -e "$latest_debug_userinstall" ]; then
      ln -sfn "$latest_debug_userinstall" xcalar-latest-userinstaller-debug
      ln -sfn "$latest_debug_userinstall" xcalar-latest-$BUILD_VERSION-userinstaller-debug
   else
      echo "$latest_debug_userinstall not found. This might be amzn only build"
   fi


   # check if build has version mismatch; if not add symlinks to build dir and RPM installers
   if "$XLRDIR/bin/detect-version-mismatch.sh"; then
      trunk_builds_no_mismatch_dir="../../BuildTrunkMatch"
      if mkdir -p "$trunk_builds_no_mismatch_dir"; then
         # symlink entire Build folder by bld num in to BuildTrunkMatch
         ln -sn "../byJob/$JOB_NAME/$BUILD_NUMBER" "$trunk_builds_no_mismatch_dir/$BUILD_NUMBER"
         # add symlinks to prod/debug RPM installers in $JOB_NAME's main dir
         test -e "$latest_prod_install" && ln -sfn "$latest_prod_install" xcalar-latest-installer-prod-match
         test -e "$latest_prod_install" && ln -sfn "$latest_prod_install" xcalar-latest-$BUILD_VERSION-installer-prod-match
         test -e "$latest_debug_install" && ln -sfn "$latest_debug_install" xcalar-latest-installer-debug-match
         test -e "$latest_debug_install" && ln -sfn "$latest_debug_install" xcalar-latest-$BUILD_VERSION-installer-debug-match
      fi
   fi
fi

# 'test' cmds can return a non-0 exit code if not all installers are being built.
# return explicit exit 0 so those codes aren't returned for the entire job if
# they are the last commands run.
exit 0
