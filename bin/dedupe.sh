#!/bin/bash
#
#
# Takes a list of files or wildcard as input and
# creates a CAS directory with hard (or soft) links
# pointing the original file to the new name
# in the CAS

set -e

dedupe() {
    if test -e 'MD5SUMS.dedupe' || test -e 'bymd5'; then
        echo >&2 "ERROR: $PWD is already deduped"
        exit 1
    fi
    xargs md5sum | tee MD5SUMS.dedupe
    local nlines
    nlines=$(wc -l < MD5SUMS.dedupe)
    if [ $nlines -eq 0 ]; then
        echo >&2 "No entries"
        return 0
    fi
    if [ $nlines -gt 100 ]; then
        if ! ((FORCE)); then
            rm -f MD5SUMS.dedupe
            echo >&2 "ERROR: Must use with --force, too many entries ($nlines)!!"
            return 1
        fi
    fi

    mkdir bymd5
    cd bymd5
    while read -r SUM _FILE; do
        FILE="../${_FILE}"
        if ! test -e ${SUM}; then
            mv ${FILE} ${SUM}
        else
            rm -f "$FILE"
        fi
        case "$LINK" in
            soft) ln -sfrnv ${SUM} ${FILE};;
            hard) ln -fv ${SUM} ${FILE};;
        esac
    done < ../MD5SUMS.dedupe
    cd - >/dev/null
}

main() {
    DIR=$PWD
    LINK=hard
    FILES_FROM=
    WC='*.rpm'
    while [ $# -gt 0 ]; do
        local cmd="$1"
        shift
        case "$cmd" in
            -C|--dir) DIR="$1"; shift;;
            --hardlink) LINK=hard;;
            -s|--symlink|--softlink) LINK=soft;;
            --files-from) FILES_FROM="$1"; shift;;
            --wildcard) WC="$1"; shift;;
            --undo) UNDO=1;;
            --force) FORCE=1;;
            -h|--help)
                echo >&2 "usage: $0 [-C|-d|--dir DIR] [--hardlink|--softlink] [--files-from -|file] [--wildcard wc] [--undo] [--force]"
                exit 0
                ;;
            *) echo >&2 "ERROR: Unrecognized option $cmd"; exit;;
        esac
    done
    cd "$DIR"
    if ((UNDO)); then
        if ! test -e MD5SUMS.dedupe; then
            echo >&2 "ERROR: MD5SUMS.dedupe not found"
            exit 1
        fi
        if ! test -d 'bymd5'; then
            echo >&2 "ERROR: bymd5 not found"
            exit 1
        fi
        while read -r SUM _FILE; do
            local orig="bymd5/${SUM}"
            if test -e "$orig"; then
                ln -fv "$orig" "${_FILE}"
            else
                echo >&2 "WARNING: $orig not found for ${_FILE}"
            fi
        done < MD5SUMS.dedupe
        rm -rf MD5SUMS.dedupe bymd5
        exit
    elif [ -n "$FILES_FROM" ]; then
        case "$FILES_FROM" in
            -*) dedupe;;
            *) dedupe < "$FILES_FROM";;
        esac
    else
        find . -type f -name "$WC" -not -path './bymd5*' | dedupe
    fi
}

main "$@"
