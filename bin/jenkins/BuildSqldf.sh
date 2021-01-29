#!/bin/bash
set -e

. bin/xcsetenv
NAME=xcalar-sqldf
BUILD_NUMBER="${BUILD_NUMBER:-0}"
export BUILD_NUMBER

JAVA_HOME=$(java_home.sh)
export JAVA_HOME

if [ -n "$JENKINS_URL" ]; then
    GIT_SPARK_BRANCH="$(awk '{print $2}' "$XLRDIR"/src/3rd/spark/spark.txt)"
    if [ "$XLRSPARK_BRANCH" != "$GIT_SPARK_BRANCH" ]; then
        echo "###########"
        echo "# Spark branch provided by Jenkins $XLRSPARK_BRANCH"
        echo "# does not match branch in XLRDIR/src/3rd/spark/spark.txt"
        echo "# $GIT_SPARK_BRANCH.  Verify that the XCE_GIT_BRANCH"
        echo "# $XCE_GIT_BRANCH and XLRSPARK_BRANCH are compatible"
        echo "###########"
        exit 1
    fi
    git clean -fxd >/dev/null
else
    git-subclone.sh src/3rd/spark/spark.txt "$XLRDIR"/spark
fi

if ! test -e "$XLRDIR/spark/.git"; then
    echo >&2 "Unable to find spark git repo"
    exit 1
fi

cd "$XLRDIR"/spark
git clean -fxd >/dev/null
cd -

# TODO: We're using 2 versions of sbt. The system one in
# /usr/bin/sbt and the one that spark downloads/runs in
# spark/build/sbt. The latter is an older version of sbt
# that doesn't recognize -no-colors
## export SBT_OPTS="-no-colors -mem 4000"
bin/install-sqldf.sh 2>&1
set -x
if [ -n "$BUILD_DIRECTORY" ]; then
   OUTDIR="${BUILD_DIRECTORY}/${JOB_NAME}/${BUILD_NUMBER}"
   mkdir -p "$OUTDIR"
   # this should provide SQLDF_VERSION
   . "$XLRDIR"/BUILD_SHA
   JAR_NAME="$NAME"-"$SQLDF_VERSION".jar
   JAR_PATH="$XLRDIR"/"$JAR_NAME"
   #Copy here which will be consumed by build-user-installer.sh
   cp "$JAR_PATH" "${OUTDIR}/"
   cp "$XLRDIR"/BUILD_SHA "${OUTDIR}/"
   md5sum -b "$JAR_PATH" | tee "${OUTDIR}/MD5SUMS"
   ###############################################
   ln -sfn "$BUILD_NUMBER" "$BUILD_DIRECTORY"/"$JOB_NAME"/lastSuccessful-"$SQLDF_VERSION"
   ln -sfn "$BUILD_NUMBER"/"$JAR_NAME" "$BUILD_DIRECTORY"/"$JOB_NAME"/"$JAR_NAME"
fi
