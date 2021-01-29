#!/bin/bash
#
# Usually called as a pre-commit hook from git before
# a commit is made. If any files are changed by the
# plugin, the commit will "fail" and you can review the
# changes the plugin made before `git add/git commit --amend`
#
# You can bypass the git pre-commit hook by setting SKIP_FORMAT
# to any value:
#
# $ SKIP_FORMAT=xyz git commit
#
# This plugin also parses cmake/modules/AddXcalar.cmake to get
# list of files to exclude from checking. The styleIgnore section
# in AddXcalar.cmake is parsed and each line is converted.
#
# set(styleIgnore
#     "Foo.cpp"
#     "Bar.cpp"
#     ...
#
# To:
#
# /Foo.cpp
# /Bar.cpp


if [ -n "$SKIP_CLANG_FORMAT" ]; then
    echo >&2 "Skipping clang-format due to SKIP_CLANG_FORMAT=$SKIP_CLANG_FORMAT"
    exit 0
fi

# These settings make it compatible with git-clang-format
STYLE=$(git config --get clangFormat.style)
CLANG_FORMAT=$(git config --get clangFormat.binary)

if [ -z "${STYLE}" ]; then
    STYLE=file
fi
if [ -z "${CLANG_FORMAT}" ]; then
    CLANG_FORMAT=clang-format
fi

STYLEARG="-style=${STYLE}"

CLANG_FORMAT_ARGS=()
while [ $# -gt 0 ]; do
    cmd="$1"
    case $cmd in
        --)
            shift
            break
            ;;
        -h | --help)
            echo >&2 "Usage: $0 [-clang-format-arg1=value --] [file1 file2 ...]"
            exit 1
            ;;
        -style=*)
            STYLEARG="$cmd"
            shift
            ;;
        --*)
            echo >&2 "ERROR: Unknown command $cmd"
            exit 1
            ;;
        -*)
            CLANG_FORMAT_ARGS+=("$cmd")
            shift
            ;;
        *) break ;;
    esac
done

if [ $# -eq 0 ]; then
    set -- $(git diff-index --cached --name-only HEAD)
fi

EXCLUDES=($(sed -r '/^set\(styleIgnore$/,/^\)$/!d;//d; s/^\s+"/\//g; s/"$//g' cmake/modules/AddXcalar.cmake 2>/dev/null))

for filn in "$@"; do
    if ! file "$filn" | grep -q 'C source'; then
        continue
    fi
    skipFormat=false
    for exclude in "${EXCLUDES[@]}"; do
        if [[ $filn =~ $exclude ]]; then
            echo >&2 "Skipped formatting of $filn, because it matched $exclude from your exclusions"
            skipFormat=true
            break
        fi
    done
    if $skipFormat; then
        continue
    fi
    ${CLANG_FORMAT} ${STYLEARG} "${CLANG_FORMAT_ARGS[@]}" -i "${filn}"
done
