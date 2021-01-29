#!/bin/bash
#
# This script parses $XLRDIR/REVIEWERS and adds 'owners' of modules to
# gerrit as reviewers

set -e

say() {
    echo >&2 "$@"
}

die() {
    local rc=${2:-1}
    say "ERROR($rc): $1"
    exit $rc
}

gerrit_config() {
    git config -f .gitreview "$@"
}

gerrit () {
    ssh -x -oLogLevel=ERROR -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no -oPort=$GERRIT_PORT $GERRIT_HOST -- gerrit "$@" </dev/null
}

if [ -z "$GERRIT_HOST" ]; then
    GERRIT_HOST=$(gerrit_config gerrit.host) \
        || die "Need to specify GERRIT_HOST or have a .gitreview"
fi

if [ -z "$GERRIT_PORT" ]; then
    GERRIT_PORT=$(gerrit_config gerrit.port) \
        || die "Need to specify GERRIT_PORT or have a .gitreview"
fi

if [ -z "$GERRIT_PROJECT" ]; then
    GERRIT_PROJECT=$(gerrit_config gerrit.project) \
        || die "Need to specify GERRIT_PROJECT or have a .gitreview"
fi
GERRIT_PROJECT="${GERRIT_PROJECT%.git}"

if ! command -v jq &>/dev/null; then
    die "jq is missing"
fi
if ! gerrit_version=$(gerrit version) || [ -z "$gerrit_version" ]; then
    die "Failed to connect to ${GERRIT_HOST}:${GERRIT_PORT}"
fi

if ! TMPDIR=$(mktemp -d --tmpdir gerrit-reviewers.XXXXXX); then
    die "Failed to create TMPDIR"
fi

while read -r dir owners; do
    if test -z "$dir" || test -z "$owners" || test "$dir" = '#'; then
        continue
    fi
    owners="$(echo $owners | sed -e 's/,/ /g')"

    name="$(basename $dir)"
    json="$TMPDIR/${name}.json"
    gerrit query status:open project:${GERRIT_PROJECT} file:"^$dir/.*" --all-reviewers --format=JSON | head -n-1 > "$json"
    if [[ $(stat --format '%s' $json) -eq 0 ]]; then
        continue
    fi
    change_reviewers=($(jq -r 'select(.|has("allReviewers"))' < $json | jq -r '{reviewers: [.allReviewers[]|.username]}' | jq -r '.reviewers|@csv'))
    change_numbers=($(jq -r 'select(.|has("allReviewers"))' < $json | jq -r '{number:.number}' | jq -r '.number'))
    change_ids=($(jq -r 'select(.|has("allReviewers"))' < $json | jq -r '{id:.id}' | jq -r '.id'))
    change_owners=($(jq -r 'select(.|has("allReviewers"))' < $json | jq -r '{owner:.owner.username}' | jq -r '.owner'))

    count=$(( ${#change_numbers[@]} - 1 ))
    for ii in `seq 0 $count`; do
        number=${change_numbers[$ii]}
        cid=${change_ids[$ii]}
        creviewers="$(echo ${change_reviewers[$ii]} | tr -d '"' | sed -e 's/,/ /g')"
        add_reviewers=()
        for owner in $owners; do
            if ! grep -q $owner <(echo "$creviewers ${change_owners[$ii]}"); then
                add_reviewers+=($owner)
            fi
        done
        if [ ${#add_reviewers[@]} -eq 0 ]; then
            continue
        fi
        for owner in "${add_reviewers[@]}"; do
            echo "gerrit set-reviewers $cid -p ${GERRIT_PROJECT} -a $owner   # change=$number dir=$dir"
            if ! gerrit set-reviewers $cid -p ${GERRIT_PROJECT} -a $owner; then
                say "WARNING: Failed to add $owner to change-id $cid (change $number)"
                say "This is most likely caused by duplicated change-ids! Please fix this by searching for the change-id on ${GERRIT_HOST}"
            fi
        done
    done
done < REVIEWERS
rm -rf $TMPDIR
