#!/bin/bash
#
# shellcheck disable=SC2086,SC1090,SC1091

touch /tmp/${JOB_NAME}_${BUILD_ID}_START_TIME

export XTRACE=1

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

. $DIR/xcalar-sh-lib


# Common set up
export XLRDIR="$PWD"
export XLRGUIDIR="${XLRGUIDIR:-${PWD}/xcalar-gui}"
export XLRINFRADIR="${XLRINFRADIR:-${XLRDIR}/xcalar-infra}"
export XLRSOLUTIONSDIR="${XLRSOLUTIONSDIR:-${XLRDIR}/xcalar-solutions}"
export JENKINS_HARNESS=1

# shellcheck disable=SC1091
. doc/env/xc_aliases

export DEFAULT_PATH=/opt/xcalar/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export PATH=$XLRDIR/bin:$XLRINFRADIR/bin:$DEFAULT_PATH
export NETSTORE="${NETSTORE:-/netstore/qa/jenkins}"
export RESULTS_DIR="${NETSTORE}/${JOB_NAME}/${BUILD_ID}"
DESIRED_ENV_FILE="${RESULTS_DIR}/jenkins-env.keys"
mkdir -p "$RESULTS_DIR"
if [ -n "$JENKINS_ENV_FILE" ] && [ "$JENKINS_ENV_FILE" != "$DESIRED_ENV_FILE" ]; then
    echo "WARNING: We seem to have inherited a stale/old/corrupt JENKINS_ENV_FILE" >&2
    echo "WARNING: Expected JENKINS_ENV_FILE=$DESIRED_ENV_FILE got $JENKINS_ENV_FILE" >&2
    echo "Using the file as specified" >&2
else
    export JENKINS_ENV_FILE="$DESIRED_ENV_FILE"
    if ! test -e "$JENKINS_ENV_FILE"; then
        dumpenv.sh -f blacklist > "${JENKINS_ENV_FILE%.*}".keys
        dumpenv.sh -f blacklist -f values > "${JENKINS_ENV_FILE%.*}".env
    fi
fi

docker_clean

wants_container=0
if grep -q '^'"$JOB_NAME"'$' "$DIR/jenkins/containerized_jobs.txt"; then
    wants_container=1
fi
if in_container; then
    if ! ((wants_container)); then
        echo >&2 "**** Oh oh $JOB_NAME isn't listed to be in a container, yet we are in one ***"
    else
        echo >&2 "**** Running $JOB_NAME in a container *****"
    fi
else
    if ((wants_container)); then
        echo >&2 "+++ $JOB_NAME wants a container +++"
        exec /bin/bash $XLRDIR/bin/with_container.sh /bin/bash "${BASH_SOURCE[0]}" "$@"
    else
        echo >&2 "+++ $JOB_NAME is not listed to be run in a container"
    fi
fi

if [ "${BUILD_ONCE}" = "1" ]; then # build once is set, let's pick up our tar
    if [ -z "${BUILD_ONCE_URL}" ]; then
        echo "BUILD_ONCE_URL is not set"
        exit 1
    fi
    bin/build-once.sh $XLRDIR -x ${BUILD_ONCE_URL}
    bin/build-once.sh $XLRDIR -f
    . bin/xcsetenv
elif [ "${JOB_NAME}" != "BuildOnce" ]; then

    xcEnvDir="${XLRDIR}/xcve"
    # Delete all of the old dev-installed python packages
    # These stale egg-links cannot be uninstalled via normal means
    # https://github.com/pypa/pip/issues/3784
    if test -d "$xcEnvDir" ; then
        find "$xcEnvDir" -name "*.egg-link" -delete
    fi

    if ! xcEnvEnterDir "${xcEnvDir}" ; then
        echo "Failed to enter venv $xcEnvDir"
        exit 1
    fi
fi

# First look in local (Xcalar) repo for a script and fall back to the one in xcalar-infra
for SCRIPT in "${XLRDIR}/bin/jenkins/${JOB_NAME}.sh" "${XLRINFRADIR}/jenkins/${JOB_NAME}.sh" "${XLRSOLUTIONSDIR}/jenkins/${JOB_NAME}.sh"; do
    if test -x "$SCRIPT"; then
        break
    fi
done

if ! test -x "${SCRIPT}"; then
    echo >&2 "No jenkins script for $JOB_NAME"
    exit 1
fi

#setup_proxy
if [ "$NOCADDY" = 1 ]; then
    echo >&2 "Skipping caddy launch, due to NOCADDY=$NOCADDY"
else
    if [ -z "$XCE_HTTPS_PORT" ]; then
        # EXECUTOR_NUMBER is defined by Jenkins to be 0 based index into
        # the number of executors the slave has
        if [ -n "$EXECUTOR_NUMBER" ]; then
            XCE_HTTPS_PORT=$((8443 + EXECUTOR_NUMBER))
        else
            XCE_HTTPS_PORT=8443
        fi
    fi
    if caddy.sh start -p $XCE_HTTPS_PORT; then
        export XCE_HTTPS_PORT
    else
        echo >&2 "WARNING: Failed to start caddy on port $XCE_HTTPS_PORT"
    fi
fi

export SBT_OPTS="-Dsbt.log.noformat=true${SBT_OPTS:+ $SBT_OPTS}"

script_directory=$(dirname -- "${SCRIPT}")
hydra_config_path="${script_directory}/${JOB_NAME}.hydra"
if test -f "$hydra_config_path"; then
    hydra_path="${XLRINFRADIR}/py_common/hydra/hydra.py"
    python ${hydra_path} --cfg ${hydra_config_path} -- "$SCRIPT" "$@"
else
    "$SCRIPT" "$@"
fi
ret=$?

if [ "$NOCADDY" = 1 ]; then
    echo >&2 "Skipping caddy stop, due to NOCADDY=$NOCADDY"
else
    caddy.sh stop -p $XCE_HTTPS_PORT
fi
# Clean up the build directory on successful build
if [ $ret -eq 0 ]; then
    rm -rf "${BUILD_DIR:-$XLRDIR/buildOut}"
fi

rm /tmp/${JOB_NAME}_${BUILD_ID}_START_TIME

exit $ret
