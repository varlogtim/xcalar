#!/bin/bash
#
# Adds a set of signed RPMs to rhel[67]-repo,
# rebuilds the rpmdb, and uploads it to GCS`

DOWNLOAD=1
UPLOAD=1
BUCKET=${BUCKET:-repo.xcalar.net}
declare -a EL6 EL7 AMZN1 RPMCOMMON UB14 UB16 UBCOMMON
declare -A PREPO
#=(
#    [el6]=
#    [el7]=()
#    [amzn1]=()
#    [amzn2]=()
#    [rpmcommon]=()
#    [ub14]=()
#    [ub16]=()
#    [ub18]=()
#    [ubcommon]=()
#)

FORCE=false
REPOBASE=${REPOBASE:-/srv/reposync}
DRYRUN=false
UPDATE_RHEL=false
CHECKSUM=''

trace() {
    (
        set -x
        "$@"
    )
}

say() {
    echo >&2 "info: $1"
}

#gsutil() {
#  echo gsutil "$@"
#}

gcs_dontcache() {
    local CACHE_CONTROL="${CACHE_CONTROL:-private,no-cache,no-store,must-revalidate,max-age=0,no-transform}"
    (
        set -x
        gsutil -m setmeta -h "Cache-Control:$CACHE_CONTROL" "$@"
    )
}

gcs_sync() {
    if $DRYRUN; then
        trace gsutil -m rsync ${CHECKSUM} -d -r -n "$@"
    else
        trace gsutil -m rsync ${CHECKSUM} -d -r "$@"
    fi
}

declare -a EL6 EL7 AMZN1 AMZN2 RPMCOMMON UB14 UB16 UB18 UBCOMMON

add_package() {
    if [[ $1 == *el6*.rpm ]]; then
        EL6+=($1)
        say "Added $1 to EL6"
    elif [[ $1 == *el7*.rpm ]]; then
        EL7+=($1)
        say "Added $1 to EL7"
    elif [[ $1 == *amzn1*rpm ]]; then
        AMZN1+=($1)
        say "Added $1 to AMZN1"
    elif [[ $1 == *amzn2*rpm ]]; then
        AMZN2+=($1)
        say "Added $1 to AMZN2"
    elif [[ $1 == *noarch*rpm ]] || [[ $1 == *.rpm ]]; then
        RPMCOMMON+=($1)
        say "Added $1 to RPMCOMMON"
    elif [[ $1 == *ub14*.deb ]]; then
        UB14+=($1)
        say "Added $1 to UB14"
    elif [[ $1 == *ub16*.deb ]]; then
        UB16+=($1)
        say "Added $1 to UB16"
    elif [[ $1 == *ub18*.deb ]]; then
        UB18+=($1)
        say "Added $1 to UB18"
    elif [[ $1 == *all*.deb ]] || [[ $1 == *.deb ]]; then
        UBCOMMON+=($1)
        say "Added $1 to UB"
    fi
    PREPO[el6]="${EL6[*]}"
    PREPO[el7]="${EL7[*]}"
    PREPO[amzn1]="${AMZN1[*]}"
    PREPO[amzn2]="${AMZN2[*]}"
    PREPO[rpmcommon]="${RPMCOMMON[*]}"
    PREPO[ub14]="${UB14[*]}"
    PREPO[ub16]="${UB16[*]}"
    PREPO[ub18]="${UB18[*]}"
    PREPO[ubcommon]="${UBCOMMON[*]}"
}

main() {
    local REPO="$1"
    shift
    local VERSTRING HOST
    if [[ $REPO =~ ^rhel6 ]] || [[ $REPO =~ ^el6 ]]; then
        VERSTRING=el6
        HOST="rhel6-repo"
    elif [[ $REPO =~ ^rhel7 ]] || [[ $REPO =~ ^el7 ]]; then
        VERSTRING=el7
        HOST="rhel7-repo"
    elif [[ $REPO =~ ^amzn1 ]]; then
        VERSTRING=amzn1
        HOST="amzn-repo"
    elif [[ $REPO =~ ^amzn2 ]]; then
        VERSTRING=amzn2
        HOST="amzn2-repo"
    elif [[ $REPO =~ ^rpmcommon ]]; then
        VERSTRING=common
        HOST=rhel-repo
    else
        echo >&2 "Unknown repo $REPO"
        exit 1
    fi

    local REPODIR=${REPOBASE}/rpm/${VERSTRING}/x86_64
    local REPODEPSDIR=${REPOBASE}/rpm-deps/${VERSTRING}/x86_64
    local GSDIR=gs://${BUCKET}/rpm/${VERSTRING}/x86_64
    local GSDEPSDIR=gs://${BUCKET}/rpm-deps/${VERSTRING}/x86_64
    local GSDIRSERVER=gs://${BUCKET}/rpm/${VERSTRING}Server/x86_64
    local GSDEPSDIRSERVER=gs://${BUCKET}/rpm-deps/${VERSTRING}Server/x86_64

    if [ $DOWNLOAD -eq 1 ]; then
        # Sanity check before we start doing anything
        if ! command -v gsutil &> /dev/null; then
            echo >&2 "ERROR: Unable to find gsutil command!"
            exit 1
        fi

        if ! gsutil ls "$GSDEPSDIR" > /dev/null; then
            echo >&2 "ERROR: Unable to list contents of $GSDIR!"
            exit 1
        fi
    fi

    local -a UNSIGNED_RPMS=()
    local -a RPMS=()
    # Make sure our RPMs are signed.
    local ALL_SIGNED=true
    if [ $# -gt 0 ]; then
        for ii in $*; do
            SIG="$(rpm -qp "${ii}" --qf '%{SIGPGP:pgpsig}\n' 2> /dev/null)"
            if [ $? -ne 0 ] || [ "$SIG" == "(none)" ]; then
                echo >&2 "ERROR: Couldn't find signature in ${ii}"
                UNSIGNED_RPMS+=($ii)
            else
                echo "${ii}: ${SIG}"
            fi
            RPMS+=(${ii})
        done
        if [ "${#UNSIGNED_RPMS[@]}" -gt 0 ]; then
            # Print unsigned RPMs to stdout so a program can parse and act on it
            echo "Unsigned: ${UNSIGNED_RPMS[*]}"
            rpm --addsign "${UNSIGNED_RPMS[@]}" || exit 1
        fi
    fi

    set -e

    if [ "$DOWNLOAD" = 1 ]; then
        if ! test -w "$REPOBASE"; then
            sudo mkdir -p "${REPOBASE}"
            sudo chown -R $(id -u) "${REPOBASE}"
            mkdir -p "${REPODEPSDIR}" "${REPODIR}"
        fi
        test -e "${REPODEPSDIR}" && gcs_sync ${GSDEPSDIR}/ ${REPODEPSDIR}/ || true
        test -e "${REPODIR}" && gcs_sync ${GSDIR}/ ${REPODIR}/ || true
    else
        echo >&2 "Skipping download"
    fi

    if [ "${#RPMS[@]}" -gt 0 ]; then
        mkdir -p "${REPODEPSDIR}/Packages"
        for ii in "${RPMS[@]}"; do
            cp -v "$ii" "${REPODEPSDIR}/Packages"
        done
    else
        echo >&2 "No added RPMs"
    fi

    if [ "$UPLOAD" = 1 ]; then
        # Rebuild the RPMdb in a local docker container
        if ! $FORCE && [[ $(find ${REPODEPSDIR} -type f | wc -l) -lt 4 ]]; then
            echo >&2 "Very suspicious that you have less than 20 files in $REPODEPSDIR. Feel free to hand edit the script to ignore"
            exit 1
        fi

        # Copy new files and rebuilt repodata to GCS el[67] for non-RHEL (eg, CentOS)
        if [ -e "${REPODEPSDIR}" ]; then
            (cd "${REPODEPSDIR}" && crunpwd ${VERSTRING/common/el7}-build createrepo --retain-old-md=3 --unique-md-filenames -v --update .)
            gcs_sync ${REPODEPSDIR}/ ${GSDEPSDIR}/
            if ((CACHEBUST)); then
                trace gcs_dontcache -r ${GSDEPSDIR}/repodata/repomd.xml || true
            fi
        fi
        if [ -e "${REPODIR}" ]; then
            #(cd "${REPODIR}" && crunpwd ${VERSTRING/common/el6}-build createrepo --unique-md-filenames -v --update .)
            gcs_sync ${REPODIR}/ ${GSDIR}/ || true
            if ((CACHEBUST)); then
                trace gcs_dontcache -r ${GSDIR}/repodata/repomd.xml || true
            fi
        fi

        if $UPDATE_RHEL && [[ $VERSTRING =~ ^el ]]; then
            # Copy new files and rebuilt repodata from GCS el[67] to GCS el[67]Server/ for RHEL systems
            gcs_sync ${GSDIR}/ ${GSDIRSERVER}/
            gcs_sync ${GSDEPSDIR}/ ${GSDEPSDIRSERVER}/
            if ((CACHEBUST)); then
                trace gcs_dontcache -r ${GSDEPSDIRSERVER}/repodata/repomd.xml
                trace gcs_dontcache -r ${GSDIRSERVER}/repodata/repomd.xml
            fi
        fi
    else
        echo >&2 "Skipping upload"
    fi
}

usage() {
    echo >&2 "usage $0 (rpmcommon|rhel6-repo|rhel7-repo|amzn1-repo|amzn2-repo) [--dry-run] [--download-only] [--upload-only] [--no-upload] [--no-download] [--update-rhel] [--check] [--] package1.rpm ..."
    echo >&2 ""
    exit 1
}

while [ $# -gt 0 ]; do
    cmd="$1"
    case "$cmd" in
        el6 | rhel6)
            _REPO=$cmd
            shift
            ;;
        el7 | rhel7)
            _REPO=$cmd
            shift
            ;;
        amzn1)
            _REPO=$cmd
            shift
            ;;
        amzn2)
            _REPO=$cmd
            shift
            ;;
        common | rpm*)
            _REPO=rpmcommon
            shift
            ;;
        all)
            _REPO="el6 el7 amzn1 amzn2 rpmcommon"
            shift
            ;;
        *-repo)
            _REPO=$cmd
            shift
            ;;
        --dryrun | --dry-run)
            DRYRUN=true
            shift
            ;;
        --download-only)
            UPLOAD=0
            shift
            ;;
        --cache-bust)
            CACHEBUST=1
            shift
            ;;
        --upload-only)
            DOWNLOAD=0
            shift
            ;;
        --no-download)
            DOWNLOAD=0
            shift
            ;;
        --no-upload)
            UPLOAD=0
            shift
            ;;
        -b | --bucket)
            BUCKET="$2"
            shift 2
            ;;
        --update-rhel)
            UPDATE_RHEL=true
            shift
            ;;
        --checksum)
            CHECKSUM='-c'
            shift
            ;;
        -f|--force)
            FORCE=true
            shift
            ;;
        -h | --help) usage ;;
        --)
            shift
            break
            ;;
        -*)
            echo >&2 "ERROR: Unknown argument $cmd"
            exit 1
            ;;
        *) break ;;
    esac
done

if [ -z "${_REPO}" ]; then
    for pkg in "$@"; do
        add_package "$pkg"
    done
    for REPO in el6 amzn1 amzn2 el7 rpmcommon; do
        pkgs="${PREPO[$REPO]}"
        if [ -n "${pkgs}" ]; then
            main "${REPO}" $pkgs
        fi
    done
else
    for REPO in ${_REPO//,/ }; do
        main "${REPO}" "$@"
    done
fi
