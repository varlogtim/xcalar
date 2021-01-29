#!/bin/bash

BUCKET=${BUCKET:-repo.xcalar.net}
LOCALREPO="/srv/reposync/apt"
GSREPO="gs://${BUCKET}/apt"
export REPREPRO_BASE_DIR="$LOCALREPO/ubuntu"
UPLOAD=1
DOWNLOAD=1
#NOWFILE="${LOCALREPO}/.now"
NOWFILE="/tmp/$(id -u)-now"
CHECKSUM=''

dist_main_dir() {
    if [ -z "$1" ]; then
        echo >&2 "ERROR: Need to pass a string to dist_main_dir"
        exit 1
    fi
    echo "$REPREPRO_BASE_DIR/dists/$1/main/binary-amd64"
}

say() {
    echo >&2 "$*"
}

usage() {
    cat >&2 <<EOF
    usage: $0 [-d|--dist (trusty, xenial, bionic or all)] [--download-only] [--no-download] [-f|--fast]
              [-n|--dry-run] [--upload-only] [--no-upload] [--bucket BUCKET] [--] package1.deb package2.deb ..."

    BUCKET=$BUCKET
    GSREPO=$GSREPO
    LOCALREPO=$LOCALREPO
    REPREPRO_BASE_DIR=$REPREPRO_BASE_DIR (used by Debian's \`reprepro\`)
EOF
    exit 1
}

parse_args() {
    while [ $# -gt 0 ]; do
        cmd="$1"
        shift
        case "$cmd" in
            -h | --help) usage ;;
            --checksum) CHECKSUM='-c';;
            -d | --dist)
                UBUNTU_CODENAME="$1"
                shift
                ;;
            -b | --bucket)
                BUCKET="$1"
                shift
                ;;
            -n | --dry-run) DRYRUN="-n" ;;
            --download-only) UPLOAD=0 ;;
            --no-download) DOWNLOAD=0 ;;
            --upload-only) DOWNLOAD=0 ;;
            --no-upload) UPLOAD=0 ;;
            --) break ;;
            -*)
                say "Unknown flag $cmd. ..."
                usage
                ;;
            *)
                set -- "$cmd" "$@"
                break
                ;;
        esac
    done
    GSREPO="gs://${BUCKET}/apt"
    if ! gsutil ls "$GSREPO" >/dev/null; then
        say "ERROR: Unable to access $GSREPO"
        exit 1
    fi
    DEBS=("$@")
}

verify_args() {
    if [ -z "$UBUNTU_CODENAME" ]; then
        UBUNTU_CODENAME="$(lsb_release -cs)"
    fi
    if [ "$UBUNTU_CODENAME" = all ]; then
        UBUNTU_CODENAME="trusty xenial bionic"
    fi
    if [[ "$UBUNTU_CODENAME" =~ trusty ]] || [[ "$UBUNTU_CODENAME" =~ xenial ]] || [[ "$UBUNTU_CODENAME" =~ bionic ]]; then
        :
    else
        say "Unsupported ubuntu distro: $UBUNTU_CODENAME"
        exit 1
    fi

    if ! test -d "$LOCALREPO"; then
        sudo mkdir -p "$LOCALREPO"
        sudo chown $(id -u):$(id -g) "$LOCALREPO"
    fi
}

main() {

    touch $NOWFILE
    parse_args "$@"
    verify_args

    set -e
    # First copy down our existing repo
    if [ "$DOWNLOAD" = 1 ]; then
        gsutil -m rsync ${DRYRUN} -d -r ${CHECKSUM} "$GSREPO" "$LOCALREPO"
    fi

    if [ "${#DEBS[@]}" -gt 0 ]; then
        # Sign any new debian packages and add them to the local repo
        for DEB in "${DEBS[@]}"; do
            for DIST in ${UBUNTU_CODENAME//,/ }; do
                reprepro --keepunusednewfiles --keepunreferencedfiles --ask-passphrase -Vb $REPREPRO_BASE_DIR includedeb "$DIST" "$DEB"
            done
        done
        #for DIST in $UBUNTU_CODENAME; do
        #    (
        #    cd $(dist_main_dir $DIST) || exit 1
        #    if [ Release -nt $NOWFILE ]; then
        #        echo >&2 "Signing additional Release files for $DIST, due to update"
        #    fi
        #    gpg --clearsign -o InRelease Release && \
        #    gpg -o Release.gpg Release || exit 1
        #    )
        #done
        # Reverse rsync from local to repo
        if [ "$UPLOAD" = 1 ]; then
            gsutil -m rsync ${DRYRUN} -d -r ${CHECKSUM} $LOCALREPO $GSREPO
            exit $?
        fi
    else
        say "Nothing to add"
        # Reverse rsync from local to repo
        if [ "$UPLOAD" = 1 ]; then
            gsutil -m rsync ${DRYRUN} -d -r ${CHECKSUM} $LOCALREPO $GSREPO
            exit $?
        fi
    fi
    say "To manually push your local repo -> remote repo:"
    say ""
    say "# export REPREPRO_BASE_DIR=$REPREPRO_BASE_DIR; cd \$REPREPRO_BASE_DIR; reprepro commmand"
    say "# gsutil -m rsync -d -r -c $LOCALREPO $GSREPO"
}

if [ $# -eq 0 ]; then
    say "No options provided, downloading only"
    set -- --download-only
fi
main "$@"
