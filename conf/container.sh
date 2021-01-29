#!/bin/bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR"/../bin/xcsetenv

if [ -n "$HOSTUSER" ] && [ -e ~/.ssh/config ]; then
    sed -i 's/User .*$/User '${HOSTUSER}'/' ~/.ssh/config
fi

test -e "$HOSTHOME"/.vault-token && ln -sfn "$HOSTHOME"/.vault-token "$HOME"/ || :
test -e "$HOSTHOME"/.aws && ln -sfn "$HOSTHOME"/.aws "$HOME"/ || :
test -e "$HOSTHOME"/.cshellrc && ln -sfn "$HOSTHOME"/.cshellrc "$HOME"/ || :
export HISTFILE=${SHARED_CACHE:-$HOME}/.bash_history_cshell
export HISTCONTROL=erasedups:ignoreboth
export HISTSIZE=100000
export HISTFILESIZE=200000
shopt -s histappend direxpand
