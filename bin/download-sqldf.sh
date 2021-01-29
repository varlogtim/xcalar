#!/bin/bash

# download-sqldf.sh - Downloads the requested version of xcalar-sqldf.jar
# based on three shell variables:
# SQLDF_BUILD_NUMBER - the Jenkins build number of a specific build of SQLDF
# SQLDF_VERSION - the latest build of a particular version of SQLDF
# SQLDF_JOB - the Jenkins job name that produced SQLDF (optional)

# A copy of this script is kept on repo.xcalar.net with a version number in the
# name for use by Dockerfiles that do not (by default) copy from the local file
# system.  When this file is updated, put a new version in the repo:
# gsutil cp bin/download-sqldf.sh gs://repo.xcalar.net/scripts/download-sqldf-v<number>.sh

# arguments:
# $1 - final file location

test -z "$SQLDF_VERSION" && test -z "$SQLDF_BUILD_NUMBER" && \
    echo >&2 "Unable to detect SQLDF version with SQLDF_VERSION or SQLDF_BUILD_NUMBER" && \
    echo >&2 "Download failed" && exit 1

set -e
[ $# -gt 0 ] || set -- xcalar-sqldf.jar

SQLDF_JOB=${SQLDF_JOB:-BuildSqldf}
if [ -z "$SQLDF_BUILD_NUMBER" ]; then
    curl -fsSL http://netstore/builds/byJob/"$SQLDF_JOB"/xcalar-sqldf-"$SQLDF_VERSION".jar -o ${1}.$$
else
    curl -fsSL http://netstore/builds/byJob/"$SQLDF_JOB"/"$SQLDF_BUILD_NUMBER"/BUILD_SHA -o ${1}.BUILD_SHA
    # this should provide the SQLDF_VERSION
    . ${1}.BUILD_SHA
    rm ${1}.BUILD_SHA
    curl -fsSL http://netstore/builds/byJob/"$SQLDF_JOB"/"$SQLDF_BUILD_NUMBER"/xcalar-sqldf-"$SQLDF_VERSION".jar -o ${1}.$$
fi

mv ${1}.$$ ${1}
