#!/bin/bash
set -e

myName=$(basename $0)

test -z "$XLRDIR" && echo >&2 "XLRDIR must be set" && exit 1

# this should provide the SCALA_VERSION and SQLDF_VERSION
. "$XLRDIR"/src/3rd/spark/BUILD_ENV

if [ -n "$JENKINS_URL" ]; then
    RAW_BRANCH_NAME="$XCE_GIT_BRANCH"
else
    RAW_BRANCH_NAME="$(git rev-parse --abbrev-ref HEAD)"
fi


# This is entirely for Jenkins-driven accounting of the locations of
# where the latest products of official branches are located.  There
# are two types of official branches:
# trunk - the latest development branch
# SQLDF-<version #> - branches such as SQLDF-1.0, SQLDF-1.1, SQLDF-2.0
#    which are official versions of SQLDF older than trunk
#
# For all non-trunk official branches, the SQLDF_VERSION in BUILD_ENV
# is assumed to match the branch name (SQLDF_VERSION=1.0 in the
# SQLDF-1.0 branch, for example.)
#
# All other branch names are assumed to be used for development, and
# are given the SQLDF_VERSION "dev".
#
# When this script finishes, a jar file and BUILD_SHA will be produced.
# The jar file has the name xcalar-sqldf-${SQLDF_VERSION}.jar.  The
# BUILD_SHA will include the value of SQLDF_VERSION.  This can then
# be used to generate the following symbolic links to identify the
# last successful build and last successful jar file for that version:
#
# lastSuccessful-${SQLDF_VERSION} - points to the build directory
# xcalar-sqldf-${SQLDF_VERSION}.jar - points to the jar file in that build
case "$RAW_BRANCH_NAME" in
    trunk|SQLDF-*)
        echo "Using SQLDF_VERSION $SQLDF_VERSION provided by BUILD_ENV"
        ;;
    *)
        SQLDF_VERSION="dev"
        ;;
esac

SPARK_BRANCH="$(awk '{print $2}' "$XLRDIR"/src/3rd/spark/spark.txt)"
SPARK_VERSION="${SPARK_BRANCH##xcalar-spark-v}"

BUILD_NUMBER=${BUILD_NUMBER:-0}

export SQLDF_VERSION SPARK_VERSION SCALA_VERSION

echo "#"
echo "# Build SQLDF_VERSION $SQLDF_VERSION with Spark v.${SPARK_VERSION} and Scala v.${SCALA_VERSION}"
echo "#"

. osid

NAME=xcalar-sqldf
TMPDIR=${TMPDIR:-/tmp}/$(id -un)-$myName
DESTDIR=${TMPDIR}/${NAME}-${SQLDF_VERSION}/rootfs
XLR_SHA="$(git rev-parse --short=8 --verify HEAD)"
XLR_SPARK_SHA="$(cd spark && git rev-parse --short=8 --verify HEAD)"


cd $XLRDIR
bin/build-xcalar-sqldf.sh -j
(cd src/sqldf/tests && ./test_sqldf.sh)
cp "src/sqldf/sbt/target/$NAME.jar" "./${NAME}-${SQLDF_VERSION}.jar"
cat > ./BUILD_SHA<<EOF
SQLDF_VERSION=${SQLDF_VERSION}
SPARK_VERSION=${SPARK_VERSION}
SPARK_BRANCH=${SPARK_BRANCH}
SCALA_VERSION=${SCALA_VERSION}
XCALAR_SHA=${XLR_SHA}
SPARK_SHA=${XLR_SPARK_SHA}
EOF
