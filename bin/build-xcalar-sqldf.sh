#!/bin/bash

# We do this here to fall back to default values set in the XCE repo.
# We expect that they will generally be set.
DEF_ENV="$XLRDIR"/src/3rd/spark/BUILD_ENV
DEF_VERSIONS=( $(source $DEF_ENV; echo $SCALA_VERSION $SPARK_VERSION) )
SCALA_VERSION="${SCALA_VERSION:-${DEF_VERSIONS[0]}}"
SPARK_VERSION="${SPARK_VERSION:-${DEF_VERSIONS[1]}}"

test -z "$XLRDIR" && echo >&2 "XLRDIR must be set" && exit 1
test -z "$XLRGUIDIR" && echo >&2 "XLRGUIDIR must be set" && exit 1
test -z "$SCALA_VERSION" && echo >&2 "SCALA_VERSION must be set" && exit 1
test -z "$SPARK_VERSION" && echo >&2 "SPARK_VERSION must be set" && exit 1

# Safety check: compare the versions of spark in SPARK_VERSION and in the spark repo.
#
# Step 1. Get the spark version from the Spark pom.xml file.
# In the first case, JENKINS_URL is set and Jenkins has checked out the
# correct git branch.  In the second, we are running outside Jenkins and
# relying on the repo and branch stored in the XCE repo to build spark.
if [ -n "$JENKINS_URL" ]; then
    POM_SPARK_VERSION="$(xmllint --xpath "/*[local-name() = 'project']/*[local-name() = 'version']/text()" ${XLRDIR}/spark/pom.xml)"
else
    SPARK_TXT=($(cat "$XLRDIR"/src/3rd/spark/spark.txt))
    XLRSPARK_GIT_REPO="${SPARK_TXT[0]}"
    XLRSPARK_BRANCH="${SPARK_TXT[1]}"

    POM_SPARK_VERSION="$(git archive --remote="${XLRSPARK_GIT_REPO}" "${XLRSPARK_BRANCH}" pom.xml | tar -xOf - | xmllint --xpath "/*[local-name() = 'project']/*[local-name() = 'version']/text()" -)"
fi

# Step 2. Compare the spark versions.
if [ "$SPARK_VERSION" != "$POM_SPARK_VERSION" ]; then
        echo "###########"
        echo "# Spark version provided by SPARK_VERSION $SPARK_VERSION"
        echo "# does not match the version in the pom.xml file retrieved"
        echo "# from the git spark branch ${POM_SPARK_VERSION}.  Verify that"
        echo "# the SPARK_VERSION and the branch set by Jenkins or"
        echo "# ${XLRDIR}/src/3rd/spark/spark.txt agree"
        echo "###########"
        exit 1
fi

set -e

SPARKDIR="${SPARKDIR:-${XLRDIR}/spark}"

xlrRepo="gs://repo.xcalar.net/deps/"
antlrJarPath="/tmp"
antlrJarFile="antlr-4.7.2-complete.jar"
antlrJar="$antlrJarPath/$antlrJarFile"
antlrGrammar="$XLRDIR/src/sqldf/sbt/src/main/java/org/xcalar/SqlBase.g4"
antlrOut="$XLRDIR/src/sqldf/sbt/src/main/java/org/xcalar/SqlParser"

origtty=$(stty -g || true)
onExit() {
    rm -f "$antlrJar"
    # sbt messes up the terminal settings for some reason so restore it on the
    # way out
    stty "$origtty" || true
}

trap onExit EXIT

ver="${SCALA_VERSION}-${SPARK_VERSION}"
declare -a sparkCommonDeps=("spark-catalyst_${ver}.jar" "spark-core_${ver}.jar"
                            "spark-sql_${ver}.jar")
declare -a sparkJdbcDeps=("antlr-runtime-3.4.jar" "bonecp-0.8.0.RELEASE.jar" "derby-10.12.1.1.jar"
                          "datanucleus-api-jdo-3.2.6.jar" "datanucleus-core-3.2.10.jar" "datanucleus-rdbms-3.2.9.jar"
                          "hadoop-common-2.6.5.jar" "hive-cli-1.2.1.spark2.jar" "hive-exec-1.2.1.spark2.jar"
                          "hive-jdbc-1.2.1.spark2.jar" "hive-metastore-1.2.1.spark2.jar" "httpcore-4.4.10.jar"
                          "httpclient-4.5.6.jar" "jdo-api-3.0.1.jar" "libfb303-0.9.3.jar" "libthrift-0.9.3.jar"
                          "spark-hive_${ver}.jar" "spark-hive-thriftserver_${ver}.jar"
                          "spark-snowflake_2.11-2.8.2-spark_2.4.jar"  "snowflake-jdbc-3.12.12.jar")

sparkLinkPrefix="../../../../spark/assembly/target/scala-${SCALA_VERSION}/jars"

usage() {
    cat << EOF
    $0 <options>
        -j      Build with JDBC support
        -x      Perform only partial Xcalar build (reuse previous build's
                Spark depencencies
EOF
}

makeLinks() {
    local linkArr=("$@")

    cd "$XLRDIR/src/sqldf/sbtsparkdeps/lib"
    for link in "${linkArr[@]}"
    do
        ln -sf "$sparkLinkPrefix/$link"
    done
    cd - > /dev/null
}

# datanucleus JDBC dependencies are designed for an OSGi environment.  So we need
# to populate the JAR with the hand-merged plugin file and manifest.
repackageJar() {
    local tmpdir=$(mktemp --tmpdir -d buildsqldf.XXXXXX)
    local targetJar="${XLRDIR}/src/sqldf/sbt/target/xcalar-sqldf.jar"
    echo "Repackaging $targetJar"
    cd "$tmpdir"
    jar -xf "$targetJar"
    cp "${XLRDIR}/src/sqldf/customjar/plugin.xml" "$tmpdir"
    cp "${XLRDIR}/src/sqldf/customjar/MANIFEST.MF" "$tmpdir/META-INF"
    rm "$targetJar"
    jar -cmf META-INF/MANIFEST.MF "$targetJar" *
    cd - >/dev/null
    rm --preserve-root --one-file-system -rf "$tmpdir"
}

genPlanServerVerInfo() {
    local rpCmd="eval git rev-parse --verify HEAD | cut -c1-8"
    local xlrVerStr=$(cat $XLRDIR/VERSION)
    local xlrMainSha=$(cd $XLRDIR && $rpCmd)
    local xlrSparkSha=$(cd $SPARKDIR && $rpCmd)

    local scalaFile=$(cat << EOF
// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// Automatically generated by $0
// DO NOT EDIT

package org.xcalar.PlanServer
object XlrVer {
  val xlrVerStr = "$xlrVerStr"
  val xlrMainSha = "$xlrMainSha"
  val xlrSparkSha = "$xlrSparkSha"
}
EOF
)
    echo "$scalaFile" > "$XLRDIR/src/sqldf/sbt/src/main/scala/org/xcalar/PlanServer/XlrVer.scala"
}

optJdbc=false
optXcalarOnly=false
while getopts "jx" opt; do
    case $opt in
        j) optJdbc=true;;
        x) optXcalarOnly=true;;
        *) usage; exit 0;;
    esac
done

curl -sSL "http://repo.xcalar.net/deps/$antlrJarFile" > $antlrJar

# Build spark-snowflake jar
cd "${SPARKDIR}/external/spark-snowflake-spark_2.4"
sbt -Dscala-2.11 -DSPARK_SCALA_VERSION=2.11.6 package

genPlanServerVerInfo

if $optXcalarOnly
then
    echo "Skipping Spark dependency build"
else
    # Empty lib directory is not checked into git
    mkdir -p "$XLRDIR/src/sqldf/sbtsparkdeps/lib/"

    # First remove old links to avoid picking up old unmanaged deps
    rm -f "$XLRDIR/src/sqldf/sbtsparkdeps/lib/"*.jar
    makeLinks "${sparkCommonDeps[@]}"
    sparkBuildOpts=""

    if $optJdbc
    then
        makeLinks "${sparkJdbcDeps[@]}"
        sparkBuildOpts="-Phive -Phive-thriftserver"
    fi

    # Requires ${XLRDIR}/bin/git-subclone.sh ${XLRDIR}/src/3rd/spark/spark.txt

    # First build our modified Spark
    (cd "$SPARKDIR" && ./build/mvn -DskipTests $sparkBuildOpts compile package)

    mv "${SPARKDIR}/external/spark-snowflake-spark_2.4/target/scala-2.11/spark-snowflake_2.11-2.8.2-spark_2.4.jar" "${SPARKDIR}/assembly/target/scala-${SCALA_VERSION}/jars/"

    # Next assemble spark and all its dependencies into single assembly JAR with
    # custom shading to support the next build
    (cd "${XLRDIR}/src/sqldf/sbtsparkdeps" && sbt assembly)
fi

java -jar "$antlrJar" "$antlrGrammar" -o "$antlrOut" -no-listener -visitor -package org.xcalar.PlanServer

# Finally build XCE/SQL/DataFrames code into a single assembly consisting of
# Spark and all its dependenies, all of our dependencies, and finally our code
(cd "${XLRDIR}/src/sqldf/sbt" && sbt assembly)

$optJdbc && repackageJar

# Build the test Spark App.  Should be binary compatible with both Spark native
# and Xcalar Spark
(cd "${XLRDIR}/src/sqldf/tests/sbt" && sbt package)