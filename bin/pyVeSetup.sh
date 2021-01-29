#!/bin/bash
# This script sets up the virtualenv at $VE from the requirements at $REQ
# When it returns, the environment may be activated with $VE/bin/activate
# and then run.

if ((XTRACE)); then
    set -x
    QUIET=
else
    QUIET=-q
fi

die() {
    echo >&2 "ERROR: $1"
    exit 1
}

have_command() {
    command -v "$1" &>/dev/null
}

sha256() {
    if have_command sha256sum; then
        sha256sum "$@"
    elif have_command shasum; then
        shasum -a 256 "$@"
    fi
}

pyversion() {
    local py="${1:-"$PREFIX"/bin/python3}"
    if ! test -x "$py"; then
        return 1
    fi
    if "$py" -c 'import sys; v=sys.version_info; print(v[0]*1000+v[1]*100+v[2])'; then
        return
    fi
    echo 0
    return 1
}

# Get the installed version number of a package/file
# eg, for xcalar-python36 => 3.6.4-76
#     /opt/xcalar/bin/python3 => 3.6.4-76
pkgversion() {
    local v
    if test -e /etc/system-release; then
        local q
        test -f "$1" && q='-qf' || q='-q'
        if ! v=$(rpm $q "$1" --qf '%{VERSION}-%{RELEASE}'); then
            return 1
        fi
    else
        local pkg
        test -f "$1" && pkg="$(dpkg -S "$1" | cut -d: -f1)" || pkg="$1"
        if ! v=$(dpkg -s "$pkg" | awk '/^Version:/{print $2}'); then
            return 1
        fi
    fi
    echo "${v%.ub1?}"
}


pyvenvbuild() {
    local pyversion pylocal clear
    pyversion="$(pkgversion $PREFIX/bin/python3)"
    pylocal="$(cat "$1"/VERSION 2>/dev/null)"
    # Check if it's just a iteration update (eg, 3.6.4-120 -> 3.6.4-150)
    # or whether there's a version change.
    if [ "${pyversion%-*}" != "${pylocal%-*}" ]; then
        clear=1
    fi
    if [ "$pyversion" != "$pylocal" ]; then
        if test -e "$1"/bin/python3; then
            test -L "$1"/bin/python3 || rm -rf "$1"
        fi
        if [ -x "$PREFIX"/bin/virtualenv ]; then
            "$PREFIX"/bin/virtualenv ${QUIET} --system-site-packages ${clear:+--clear} "$1" \
                && echo "$pyversion" >"$1"/VERSION \
                && rm -f "$1"/SHASUMS
        else
            mkdir -p "$1" \
                && "$PREFIX"/bin/python3 -m venv --system-site-packages ${clear:+--clear} "$1" \
                && "$1"/bin/python -m pip install ${QUIET} -U pip \
                && "$1"/bin/python -m pip install ${QUIET} -U setuptools \
                && echo "$pyversion" >"$1"/VERSION \
                && rm -f "$1"/SHASUMS
        fi
    fi
}

pyvenvclean() {
    test -e "$1" && rm -rf "${1:?Root protection}"/*
    # Since we're deleting our virtualenv, let's make sure the build system
    # is aware that any xcalar packages need to be reinstalled
    if [ -d "${BUILD_DIR}" ]; then
        find "${BUILD_DIR}" -name "dev_install.stamp" -delete
    fi
}

pysetenv() {
    if [ "$1" == -u ]; then
        unset PIP_FIND_LINKS PIP_TRUSTED_HOST
    else
        local pyv="$(pyversion)"
        # eg, py3.6-el7
        local pyvosid="py${pyv:0:1}.${pyv:1:1}-$(osid)"
        export PIP_FIND_LINKS="${PIP_FIND_LINKS:+$PIP_FIND_LINKS }http://${NETSTORE_HOST:-netstore}/infra/wheels/${pyvosid}/ http://${NETSTORE_HOST:-netstore}/infra/wheels/index.html"
        export PIP_TRUSTED_HOST="${PIP_TRUSTED_HOST:+$PIP_TRUSTED_HOST }${NETSTORE_HOST:-netstore}"
    fi
}

pyvemain() {
    if [ -z "$1" ]; then
        echo >&2 "Must specify target venv dir"
        return 1
    fi
    VE="$1"
    shift 1
    local -a PIP_CMD=()
    local -a DEPS=()
    PKG="xcalar-python36"
    PREFIX="${PREFIX:-/opt/xcalar}"
    while [ $# -gt 0 ]; do
        local cmd="$1"
        shift
        case "$cmd" in
            -r)
                test -e "$1" || die "$1 not found"
                DEPS+=("$1")
                if ! diff <(grep -v torch "$PREFIX"/share/doc/xcalar-python36-*/requirements.txt 2>/dev/null) <(grep -v torch "$1") >/dev/null; then
                    PIP_CMD+=(-r "$1")
                fi
                shift
                ;;
            -c)
                test -e "$1" || die "$1 not found"
                DEPS+=("$1")
                PIP_CMD+=(-c "$1")
                shift
                ;;
            *) PIP_CMD+=("$cmd") ;;
        esac
    done

    # This script should operate in a clean environment
    if type -t deactivate >/dev/null; then
        deactivate || true
    fi

    # Checks whether requirements.txt shasums has changed
    if pyvenvbuild "$VE" && (cd "$XLRDIR" && sha256 -c "$VE"/SHASUMS >/dev/null 2>&1); then
        # If not try to do trivial accept (common case)
        if . "$VE"/bin/activate; then
            return 0
        fi
    fi
    echo >&2 "Installing venv packages into ${VE#$PWD/} ..."
    set +e
    pysetenv

    for ((ii = 1; ii <= 3; ii++)); do
        if ! pyvenvbuild "$VE"; then
            pyvenvclean "$VE"
            continue
        fi
        timeout ${PIP_TIMEOUT:-1200s} "$VE"/bin/python -m pip install ${QUIET} "${PIP_CMD[@]}"
        rc=$?
        case "$rc" in
            0)
                (cd "$XLRDIR" && sha256 "${DEPS[@]}" >"$VE"/SHASUMS)
                echo >&2 "Success"
                return 0
                ;;
            130)
                echo >&2 "Interrupted."
                return 2
                ;;
            124)
                echo >&2 "Timed out. Retrying."
                ;;
            *)
                echo >&2 "Pip encountered error $rc. Retrying"
                ;;
        esac
        echo >&2 "$ii - Failed to 'pip install ${PIP_CMD[*]}' ... attempt #$ii"
        sleep $((2 * ii))
        # Pip locks up trying to open a file in the http cache
        echo >&2 "$ii - Removing pip http cache ..."
        rm -rf ~/.pip/cache/http ${XDG_CACHE_HOME:-$HOME/.cache}/pip/http
        echo >&2 "$ii - Clearing current installation ..."
        pyvenvclean "$VE"
        if [ -n "$http_proxy" ]; then
            echo >&2 "$ii - Unsetting http_proxy=$http_proxy"
            unset http_proxy
            unset https_proxy
        fi
        if [ $ii -ge 2 ]; then
            echo >&2 "$ii - Removing rest of pip cache ..."
            rm -rf ~/.pip/cache ${XDG_CACHE_HOME:-$HOME/.cache}/pip
            echo >&2 "$ii - Disable remote pip cache ... "
            pysetenv -u
        fi
    done
    return 1
}

if [ "$(basename -- "$0")" = "$(basename -- "${BASH_SOURCE[0]}")" ]; then
    set -e
    pyvemain "$@"
    exit $?
fi
