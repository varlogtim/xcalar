#!/bin/bash
#
# Trigger a remote jenkins job, after pushing your current commit to
# the staging git repo (prototype or origin on git@git:/gitrepos)
# By default, BuildCustom is triggered, but you can specify the job
# to execute.
#
set -e
REPO="${REPO:-origin}"
if [ -z "$REPO" ]; then
    REPO=$(git remote -v | grep git@git:/gitrepos | head -1 | awk '{print $1}')
fi
CURBRANCH="$(git rev-parse --abbrev-ref HEAD)"
BRANCH="${BRANCH:-user/${LOGNAME}-${CURBRANCH}}"
XD_GIT_BRANCH=${XD_GIT_BRANCH:-trunk}
JENKINS_URL="${JENKINS_URL:-https://jenkins.int.xcalar.com}"
TOKEN="${TOKEN:-build1t}"
JOB_NAME="${JOB_NAME:-BuildCustom}"
CAUSE="cause-text=Triggered+build+by+${USER}+from+$(hostname -s)"
DRYRUN=false

# via https://gist.github.com/cdown/1163649
urlencode() {
    # urlencode <string>
    (
        set +x
        LC_COLLATE=C

        local length="${#1}"
        for ((i = 0; i < length; i++)); do
            local c="${1:i:1}"
            case $c in
                [a-zA-Z0-9.~_-]) printf "$c" ;;
                *) printf '%%%02X' "'$c" ;;
            esac
        done
    )
}

usage() {
    cat <<EOF
    usage: $0 [-r|--repo <prototype repo>] [-u|--url https://jenkins-url] [-j|--job JOB_NAME (default: $JOB_NAME)]
              [-n|--dryrun] [optional PARAM1=value PARAM2=value ...]

    Trigger a remote jenkins job, after pushing your current commit to
    the staging git repo (prototype or origin on git@git:/gitrepos)
    By default, BuildCustom is triggered, but you can specify the job
    to execute.
EOF
    exit 1
}

while [ $# -gt 0 ]; do
    cmd="$1"
    case "$cmd" in
        -h | --help) usage ;;
        -r | --repo)
            REPO="$2"
            shift 2
            ;;
        -u | --url)
            JENKINS_URL="$2"
            shift 2
            ;;
        -j | --job)
            JOB_NAME="$2"
            shift 2
            ;;
        -n | --dryrun)
            DRYRUN=true
            shift
            ;;
        --)
            shift
            break
            ;;
        -*)
            echo >&2 "ERROR: Unknown flag $cmd"
            usage
            ;;
        *)
            break
            ;;
    esac
done

if [ -z "$REPO" ]; then
    echo >&2 "Couldn't determine your default origin/prototype remote name. Please specify via --repo $0 ..."
    exit 1
fi

if [ "$(git rev-parse HEAD)" != "$(git rev-parse ${REPO}/${BRANCH} 2>/dev/null)" ]; then
    if $DRYRUN; then
        echo git push -f $REPO HEAD:${BRANCH}
    else
        git push -f $REPO HEAD:${BRANCH}
    fi
fi

PARAMS="XCE_GIT_BRANCH=$(urlencode $BRANCH)&XD_GIT_BRANCH=$(urlencode $XD_GIT_BRANCH)"
while [ $# -gt 0 ]; do
    flag="$1"
    shift
    KV=($(echo "$flag" | sed -r 's/^([A-Za-z0-9_]+)=(.*)$/\1 \2/g'))
    if [ -n "${KV[0]}" ] && [ -n "${KV[1]}" ]; then
        PARAMS="$PARAMS&${KV[0]}=$(urlencode ${KV[1]})"
    fi
done

TMP="$(mktemp /tmp/jenkins-remote.XXXXXX)"
if $DRYRUN; then
    echo "curl -w '%{http_code}\n' -L \"$JENKINS_URL/job/${JOB_NAME}/buildWithParameters?token=${TOKEN}&${CAUSE}&${PARAMS}\" -o $TMP"
else
    OUTPUT="$(curl -w '%{http_code}\n' -L "$JENKINS_URL/job/${JOB_NAME}/buildWithParameters?token=${TOKEN}&${CAUSE}&${PARAMS}" -o "$TMP")"
    if [ "${OUTPUT:0:1}" != 2 ]; then
        echo >&2 "Deployment failed with code $OUTPUT"
        echo "FILE=$TMP"
        exit 1
    fi
    echo >&2 "$JOB_NAME trigger "
    rm -f $TMP
fi
