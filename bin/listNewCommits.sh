#! /bin/bash

# This script shows all commits in the development branch that have not been cherry-picked into the stable branch
# Please note that all commits in the stable branch since the common ancestor must have the (Cherry picked from <git-sha>) comment
# This implies that all commits from the stable branch since the common ancestor must have been cherry-picked
# This requirement is to prevent any potential work loss and/or confusion

if [ $# -lt 2 -o $# -gt 3 ]; then
    echo "Usage: $0 <development-branch> <stable-branch> [all | not-in-dev-branch | in-dev-branch]"
    exit 0
fi

DIR=`dirname ${BASH_SOURCE[0]}`
XLRROOT=$DIR/..
pathToUtilsLib="$XLRROOT/bin"

showNotInDevBranch="true"
showInDevBranch="true"

if [ $# -le 3 ]; then
    if [ "$3" = "not-in-dev-branch" ]; then
        showInDevBranch="false"
    elif [ "$3" = "in-dev-branch" ]; then
        showNotInDevBranch="false"
    fi
fi
. $pathToUtilsLib/utils.sh

devBranch=$1
stableBranch=$2

commonAncestor=`git merge-base $devBranch $stableBranch`

devNumCommitsSinceCommonAncestor=`git rev-list $commonAncestor..$devBranch | wc -l`

if [ "$devNumCommitsSinceCommonAncestor" -gt 100 ]; then
    printInColor "red" "WARNING! $devBranch and $stableBranch deviates by $devNumCommitsSinceCommonAncestor commits"
    echo "Are you sure you wish to continue [y/N]"
    read userInput
    if [ "$userInput" != "y" -a "$userInput" != "Y" ]; then
        exit 0
    fi
fi

inDevBranch=""
notInDevBranch=""
commitsList=`git rev-list $commonAncestor..$devBranch`
while read commit; do
    match=`git lg --grep="$commit" $stableBranch | wc -w`
    if [ "$match" -eq 0 ]; then
        notInDevBranch="$commit $notInDevBranch"
    else
        inDevBranch="$commit $inDevBranch"
    fi
done <<< "$commitsList";

if [ "$showInDevBranch" = "true" ]; then
    printInColor "green" "Commits found in $stableBranch"
    echo "=================================================="

    for commit in $inDevBranch; do
        git lg -n 1 $commit
    done

    echo ""
fi

if [ "$showNotInDevBranch" = "true" ]; then
    printInColor "red" "Commits NOT found in $stableBranch"
    echo "===================================================="

    for commit in $notInDevBranch; do
        git lg -n 1 $commit
    done
fi

