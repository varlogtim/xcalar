#!/bin/bash
# Pass it a filename containing
# git-repo  sha1ref
# or pass the two as arguments
# The sha1 doesn't have to be a sha it could be any ref (tag, branch, etc)

set -e

if test -f "$1"; then
    REPOSHA=($(cat "$1"))
    REPO="${REPOSHA[0]}"
    SHA1="${REPOSHA[1]}"

    REPODIR="${2:-${1%.*}}"
else
    REPOSHA=("$1" "$2")
    REPO="${REPOSHA[0]}"
    SHA1="${REPOSHA[1]}"

    REPODIR="${3:-$(basename "$REPO" .git)}"
fi

git_url2dir () {
    local git_base="$HOME/.git-archiver/repos"
    mkdir -p "$git_base"
    echo "$git_base/$(echo "$1" | sed -e 's#/#%#g')"
}

# Keep a local mirror in $HOME that we can use as a reference repo
# for quick subsequent clones.

REPOREF="$(git_url2dir "$REPO")"
if ! test -d "$REPOREF"; then
    git clone --mirror "$REPO" "$REPOREF"
else
    (cd $REPOREF && git fetch)
fi

if test -d "$REPODIR"; then
    cd "$REPODIR"
    git remote update
else
    git clone -q --reference "$REPOREF" "$REPO" "$REPODIR"
    cd "$REPODIR"
fi
git checkout -q -f "$SHA1"
