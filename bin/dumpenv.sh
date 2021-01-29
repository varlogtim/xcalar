#!/bin/bash
#
# dumpenv.sh - dump out the given (or current) environemt while
# filtering any possible problematic ones. The output can be used
# as an argument to docker's --env-file
#
# shellcheck disable=SC1001,SC2086

BLACKLIST_PREFIX='_fzf _OLD_VIRTUAL_ LC_ LESS PIP_ QT_ QT4 XDG_ J2 JAVA_ HIST BASH_FUNC_ GTK VTE_ INFINALITY GNOME GPG_'
BLACKLIST_WHOLEW='_ CDPATH DBUS_SESSION_BUS_ADDRESS GOBIN GOPATH GOROOT HOME HOSTNAME PROMPT_COMMAND '
BLACKLIST_WHOLEW+='LOGNAME LS_COLORS MAIL MANPATH NODE_PATH USERNAME '
BLACKLIST_WHOLEW+='TMPDIR TMP PWD OLDPWD '
BLACKLIST_WHOLEW+='PYTHONHOME _OLD_PYTHONHOME _OLD_PATH '
BLACKLIST_WHOLEW+='SHELL SHLVL SSH_CLIENT SSH_CONNECTION SSH_TTY TERM TMUX USER LANG LANGUAGE'

dumpenv_usage() {
    cat <<EOF
    usage: $0 [-] [-c|--clear] [-f|--filter default|values|shvalues|blacklist|keys|sort]

    Filters the environment (or stdin if the first arg is '-') to produce
    a list of env keys (or key=value pairs) that can be safely passed to
    another process. The main use case is to generate a list for use with
    docker's --env-file feature.
EOF
}

needs_quotes() {
    [[ "$1" == *$'\n'* ]] || [[ "$1" == *' '* ]] || [[ "$1" == *$'\t'* ]]
}

filter_blacklist() {
    # Replace ' ' with '|' so we get a long chain of "or"'s
    # for grep. `grep '(WORD1|WORD2|WORD3)'
    local BLW="${BLACKLIST_WHOLEW// /|}"
    local BLP="${BLACKLIST_PREFIX// /|}"
    grep -Ev "^($BLP)" | grep -Ev "^($BLW)\$"
}

filter_keys() {
    sed -r 's/^([^=]+)=.*$/\1/'
}

filter_sort() {
    LC_ALL=C sort
}

filter_default() {
    filter_blacklist
}

filter_shvalues() {
    local key
    while read -r key; do
        printf '%s=%q\n' "$key" "${!key}"
    done
}

filter_values() {
    local key
    while read -r key; do
        if needs_quotes "${!key}"; then
            printf "%s='%s'\n" "$key" "${!key}"
        else
            printf "%s=%s\n" "$key" "${!key}"
        fi
    done
}

filter() {
    if [ "$(type -t filter_$1)" != function ]; then
        echo "ERROR: Unknown filter: $1" >&2
        return 1
    fi
    filter_$1
}

envkeys() {
    python -c 'from __future__ import print_function; import os; print("\n".join(os.environ.keys()))' | grep -v '^BASH_FUNC_'
}

dumpenv_main() {
    export -f envkeys
    local input='envkeys'

    if [ "$1" == '-h' ] || [ "$1" == '--help' ]; then
        dumpenv_usage
        return
    elif [ "$1" == - ]; then
        input=''
        shift
    fi
    local chain='filter_default'
    while [ $# -gt 0 ]; do
        local cmd="$1"
        shift
        case "$cmd" in
            -c|--clear)
                chain=''
                ;;
            -f|--filter)
                if [ "$(type -t filter_$1)" != function ]; then
                    echo >&2 "ERROR: Unknown filter: $1"
                    return 1
                fi
                chain="${chain:+$chain | }filter_$1"
                shift
                ;;
            *)
                echo "Unknown argument: $cmd" >&2
                dumpenv_usage >&2
                exit 1
                ;;
        esac
    done
    eval ${input:+$input |} $chain
}

dumpenv_main "$@"
