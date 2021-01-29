#!/bin/bash
#
# Usage: gdbcore.sh [-c archive.tar.gz] [-e] core1 core2...
#    -e core      print matching ELF name
#    -c archive.tar.gz create archive with coredump + elf + gdb output

# Given a coredump, find the elf
elfname () {
    local core="$1" elf=
    local coredir="$(dirname $core)" program="$(basename $core | cut -d'.' -f2- | sed -r -e 's/\.[0-9]+//g')"
    local coreelf=$(readelf -n "$core" | sed -r -e 's/^[ \t]*//g' | sort | grep '^/' | grep $program | uniq | head)
    if test -f "$coreelf"; then
        echo "${coreelf#$PWD/}"
        return 0
    fi
    elf="$(file "$core" | grep -Eow "(execfn|file): ([^,]+)" | cut -d' ' -f2 | tr -d "'")"
    if [ -z "$elf" ]; then
        elf="$(file $core | grep 'core file' | grep -Eow "from '([^ ]*)" | cut -d"'" -f2)"
        if [ -z "$elf" ]; then
            elf="$(command -v $program)"
        fi
    fi
    if test -f "$elf"; then
        echo "$elf"
        return 0
    elif test -f "$coredir/$program"; then
        echo "$coredir/$program"
        return 0
    elif test -f "$coredir/$elf"; then
        echo "$coredir/$elf"
        return 0
    else
        if command -v "$program" 2>/dev/null; then
            return 0
        fi
    fi

    return 1
}

# Given an elf and a core, print some info backtrace
backtrace () {
    #gdb --batch -q -ex 'info target' -ex 'info sharedlibrary' -ex 'info registers' -ex 'thread apply all bt full' -ex 'q' -- "$@"
    echo "#### Analyzing $* #####"
    gdb --batch -q -ex 'info registers' -ex 'thread apply all bt full' -ex 'thread apply all bt' -ex 'bt' -ex 'q' -- "$@"
    echo "#### Done with $* #####"
    echo
}

is_core () {
   file "$1" | grep -q 'core file'
}

usage () {
    cat <<EOF
    usage: $0 [[-c archive.tar.gz [-d]] | -p ] [-e] core1 core2...

     -p                  Print all found coredumps
     -e                  Print matching ELF name
     -c archive.tar.gz   Create archive with coredump + elf + gdb output
     -d                  Delete files after archiving

    if no core files are supplied, $0 will search the current directory tree.
EOF
    exit 1
}

test_gdbcore () {
    local tarlisting=$XLRDIR/src/data/gdbcore-1-listing.txt
    local tarfile=$XLRDIR/src/data/gdbcore-1.tar.gz
    test -e "$tarfile" || curl -L http://repo.xcalar.net/deps/$(basename $tarfile) -o $tarfile
    test -e "$tarlisting" || curl -L http://repo.xcalar.net/deps/$(basename $tarlisting) -o $tarlisting

    local TMPDIR=/tmp/gdbcore-$$
    mkdir -p $TMPDIR
    tar zxf $tarfile -C $TMPDIR
    mkdir -p /var/tmp/xcalar-root
    rm -vf /var/tmp/xcalar-root/*
    mv $TMPDIR/var/tmp/xcalar-root/* /var/tmp/xcalar-root
    local CWD=`pwd`
    local RELATIVECWD="${CWD#/}"
    (cd $TMPDIR && gdbcore.sh -c $CWD/gdbcore.tar.gz $XLRDIR $PWD)
    echo "1..1"
    if ! diff <(tar tvf gdbcore.tar.gz | awk '{print $1,$2,$3,$(NF)}' | sed -e "s@$RELATIVECWD/@@g") <(awk '{print $1,$2,$3,$(NF)}' $tarlisting); then
        echo "not ok 1 # SKIP tar listing time stamp diff"
    else
        echo "ok 1 - tar listing"
    fi
    rm -rf $TMPDIR
}

find_cores () {
    local FILES=() ARG=
    test -d "$1" && cd "$1" || cd "$(dirname $1)"
    FILES=($(find "$@" -name 'core.*.[0-9]*' \( -not -path './xcalar-gui/*' -a -not -path '*/.git/*' \)))
    if [ "${#FILES[@]}" -eq 0 ]; then
        echo >&2 "No core files found in $*"
        exit 0
    fi
    for ARG in "${FILES[@]}"; do
        if ! is_core "$ARG"; then
            continue
        fi
        ARG="${ARG#./}"
        ARG="${ARG#$PWD/}"
        echo "$ARG"
    done
    cd - >/dev/null
}


TARFLAGS=()
while getopts "hepdtc:" opt; do
    case $opt in
        h) usage;;
        e) ELFNAMES=1;;
        p) PRINTCORES=1;;
        c) TARARCHIVE="$OPTARG";;
        t) test_gdbcore; exit $?;;
        d) DELETECORES=1;;
        --) break;;
        *) echo >&2 "Unknown argument $optarg"; usage;;
    esac
done

shift $((OPTIND-1))

[ $# -eq 0 ] && set -- `pwd`
CORES=($(find_cores "$@"))

if [ "$PRINTCORES" = 1 ]; then
    for ARG in "${CORES[@]}"; do
        echo "${ARG#./}"
    done
    if [ "$ELFNAMES" = 1 ]; then
        for ARG in "${CORES[@]}"; do
            elfname "${ARG#./}"
        done | sort | uniq
    fi
elif [ "$ELFNAMES" = 1 ]; then
    for ARG in "${CORES[@]}"; do
        elfname "${ARG#./}"
    done | sort | uniq
else
    ARCHIVE=()
    ELF=
    for ARG in "${CORES[@]}"; do
        if ! is_core "$ARG"; then
            if is_core "${ARG%.txt}"; then
                continue
            fi
            echo >&2 "$0: WARNING: $ARG is not a core dump"
            continue
        fi
        ARG="${ARG#./}"
        ARG="${ARG#$PWD/}"
        CORETXT="$(basename $ARG).txt"
        ARCHIVE+=("$ARG")
        if ELF="$(elfname "$ARG")" && test -x "$ELF"; then
            ARCHIVE+=("${ELF#$PWD/}")
            if test -L "$ELF"; then
                REALELF="$(readlink -f $ELF)"
                REALELF="${REALELF#$PWD/}"
                ARCHIVE+=("$REALELF")
            fi
            backtrace "$ELF" "$ARG" 2>&1 | tee "$CORETXT"
            ARCHIVE+=("$CORETXT")
        fi
    done
    if test -n "$TARARCHIVE" && test "${#ARCHIVE[@]}" -gt 0; then
        echo >&2 "Creating $TARARCHIVE"
        COMPRESS_PROG=gzip
        case "$TARARCHIVE" in
            *.gz) COMPRESS_PROG=$(command -v pigz 2>/dev/null || which gzip);;
            *.bz2) COMPRESS_PROG=$(command -v pbzip2 2>/dev/null || which bzip2);;
        esac
        UNIQ=$(echo "${ARCHIVE[@]}" | tr ' ' '\n' | sort | uniq)
        env > env.txt
        osid > osid.txt
        if tar Scf "$TARARCHIVE" --numeric-owner --mode=oga=rw --ignore-failed-read ${COMPRESS_PROG:+--use-compress-prog=$COMPRESS_PROG} env.txt osid.txt $UNIQ; then
            if [ "$DELETECORES" = 1 ]; then
                rm -v $(echo "$UNIQ" | grep 'core\.')
            fi
            rm env.txt osid.txt
        else
            echo >&2 "Failed to tar files"
            exit 1
        fi
    fi
fi
