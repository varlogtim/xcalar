#!/bin/bash

say () {
    echo >&2 "$*"
}

die () {
    res=$1
    shift
    echo >&2 "ERROR:$res: $*"
    exit $res
}

in_container() {
    [ -z "$container" ] || return 0
    local cid
    if /is_container 2>/dev/null ; then
        return 0
    fi
    return 1
}

update_local_config () {
    local setting="$1"
    local loc="$2"
    local key="$(echo "$setting" | cut -d = -f1)"
    local last_key="$(grep -E "^${key}=" "$loc" | tail -1)"

    if [ "$last_key" != "$setting" ]; then
        echo "$setting" >> "$loc"
    fi
}

extract_config="0"
run_config="0"
verify_config="0"
config_file=""

set -x
say "### Parsing input flags"

while [ $# -gt 0 ]; do
    cmd="$1"
    case "$cmd" in
        -c|--config) extract_config="1";;
        -ve|--verify) verify_config="1";;
        -s|--sudo) run_config="1";;
        -f|--config_file) config_file=$(readlink -f "$2"); shift;;
        -u|--upgrade_config_file) upgrade_file=$(readlink -f "$2"); shift;;
        -o|--opt_defaults_file) opt_defaults_file=$(readlink -f "$2"); shift;;
        *) break;;
    esac
    shift
done

say "### Checking environment and operating system version"

# This should be passed from
test -n "$ROOTDIR" || ROOTDIR="$(cd "$(dirname ${BASH_SOURCE[0]})" && pwd)"
test -n "$MKSHARPWD" && cd "$MKSHARPWD"

. $ROOTDIR/osid > /dev/null
VERSTRING="$(_osid --full)"
export VERSTRING

rm ${ROOTDIR}/osid

case ${OSID_NAME} in
    el|rhel)
        distro="el${OSID_VERSION}"
        ;;
    *)
        distro="$VERSTRING"
        ;;
esac

say "### Operating system is $VERSTRING"

CONFIG_TAR="$(find $ROOTDIR -mindepth 1 -maxdepth 1 -name configuration_tarball-*.${distro}.tar.gz -print)"

if test "$extract_config" = "1"; then
    say "### Extracting configuration tarball"
    cp -p $CONFIG_TAR $MKSHARPWD
    rc=$?

    exit $rc
fi

set -o pipefail

if test "$run_config" = "1" -a -f $CONFIG_TAR; then
    test -n "$config_file" && mkdir -p ${ROOTDIR}/config && \
        cp -p $config_file ${ROOTDIR}/config/setup.txt

    say "### Running pre-config.sh"

    tar xzf $CONFIG_TAR -C $ROOTDIR && \
        sudo ${ROOTDIR}/config/pre-config.sh
    rc=$?

    exit $rc
fi

if test "$verify_config" = "1" -a -f $CONFIG_TAR; then
    test -n "$config_file" && mkdir -p ${ROOTDIR}/config && \
        cp -p $config_file ${ROOTDIR}/config/setup.txt

    say "### Running verify.sh"

    tar xzf $CONFIG_TAR -C $ROOTDIR && \
        ${ROOTDIR}/config/verify.sh
    rc=$?

    exit $rc
fi

rm -f ${ROOTDIR}/configuration_tarball-*.tar.gz

set -e

if test -e "${ROOTDIR}/user-installer.tar.gz"; then
    say "### Extracting Xcalar distribution and test data"
    tar xzf "${ROOTDIR}/user-installer.tar.gz" -C $ROOTDIR || \
        die $? "Extraction of Xcalar software failed"
    tar xzf "${ROOTDIR}/test-data.tar.gz" -C "${ROOTDIR}/opt/xcalar" || \
        die $? "Extraction of Xcalar test data failed"

    rm -f ${ROOTDIR}/user-installer.*.tar.gz ${ROOTDIR}/test-data.tar.gz
else
    die 1 "${ROOTDIR}/user-installer.tar.gz could not be found"
fi

set +e

say "### Checking for presets and initializing config variables"

test -e "$ROOTDIR/etc/default/xcalar" && . "$ROOTDIR/etc/default/xcalar"
if [ -n "$opt_defaults_file" ] && [ -f "$opt_defaults_file" ]; then
    . "$opt_defaults_file"
fi

# Could also check for dev $XLRDIR here and keep XLRDIR , adjust PATH, etc
export XLRDIR="$ROOTDIR/opt/xcalar"
export PATH="/usr/sbin:/usr/bin:/sbin:/bin:$XLRDIR/bin:$XLRDIR/scripts:$PATH"
export LD_LIBRARY_PATH="$ROOTDIR/opt/xcalar/lib:$LD_LIBRARY_PATH"
if test -n "$XCE_CONFIG" && test -e "$XCE_CONFIG"; then
    export XCE_CONFIG
elif test -e "$XLRDIR/src/data/$(hostname -s).cfg"; then
    export XCE_CONFIG="$XLRDIR/src/data/$(hostname -s).cfg"
else
    export XCE_CONFIG="$ROOTDIR/etc/xcalar/default.cfg"
fi

## Only necessary if we're root
if test "$(id -u)" -eq 0; then
    export XCE_USER="${XCE_USER:-xcalar}"
    export XCE_GROUP="${XCE_GROUP:-xcalar}"
fi

# XCE_WORKDIR must align with the WorkingDirectory in xcalar-usrnode.service
SYSTEMD_UNIT_DIR="${SYSTEMD_UNIT_DIR:-/lib/systemd/system}"
XCE_USRNODE_UNIT="${XCE_USRNODE_UNIT:-xcalar-usrnode.service}"
if [ -f "${SYSTEMD_UNIT_DIR}/${XCE_USRNODE_UNIT}" ]; then
    export XCE_WORKDIR="$(grep 'WorkingDirectory' "${SYSTEMD_UNIT_DIR}/${XCE_USRNODE_UNIT}" | cut -d '=' -f2)"
elif test "$(id -u)" -eq 0; then
    export XCE_WORKDIR="$ROOTDIR/var/tmp/$XCE_USER-root"
else
    export XCE_WORKDIR="$ROOTDIR/var/tmp/$(id -un)-root"
fi

export XCE_DEFAULT="${XCE_DEFAULT:-$ROOTDIR/etc/default/xcalar}"
export XCE_HOME="${XCE_HOME:-$ROOTDIR/var/opt/xcalar}"
export XCE_USER_HOME="${XCE_USER_HOME:-/home/xcalar}"
export XCE_LOGDIR="${XCE_LOGDIR:-$ROOTDIR/var/log/xcalar}"
export XLRGUIDIR="${XLRGUIDIR:-$ROOTDIR/opt/xcalar/xcalar-gui}"
export XCE_LICENSEDIR="${XCE_LICENSEDIR:-$ROOTDIR/etc/xcalar}"
export XCE_LICENSEFILE="${XCE_LICENSEFILE:-$XCE_LICENSEDIR/XcalarLic.key}"
export XCE_SUPERVISOR_CONF="${XCE_SUPERVISOR_CONF:-$ROOTDIR/etc/xcalar/supervisor.conf}"
export XCE_CADDYFILE="${XCE_CADDYFILE:-$ROOTDIR/etc/xcalar/Caddyfile}"
export XCE_HTTP_ROOT="${XCE_HTTP_ROOT:-$ROOTDIR/var/www}"
export XCE_HTTP_PORT="${XCE_HTTP_PORT:-80}"
export XCE_HTTPS_PORT="${XCE_HTTPS_PORT:-8443}"
export PYTHONHOME="${PYTHONHOME:-$ROOTDIR/opt/xcalar}"
export XCE_CGCTRL_CMD="${XCE_CGCTRL_CMD:-$ROOTDIR/bin/cgroupControllerUtil.sh}"
export XCE_XCALAR_UNIT="${XCE_XCALAR_UNIT:-xcalar.service}"

say "### Creating directories"

mkdir -p $XCE_LOGDIR $XCE_WORKDIR $XCE_HOME
chmod 755 $XCE_LOGDIR

if ! test -e "$XCE_CONFIG"; then
    say "INFO: Generating a new config in $XCE_CONFIG based on  your hostname: `hostname -f`"
    say "INFO: You can edit this file to change any settings"
    genConfig.sh $ROOTDIR/etc/xcalar/template.cfg - `hostname -f` | sed -e 's@^Constants.XcalarRootCompletePath=.*$@Constants.XcalarRootCompletePath='${XCE_HOME}'@g' > $XCE_CONFIG
    echo "" >> $XCE_CONFIG
    echo "Constants.XcalarLogCompletePath=${XCE_LOGDIR}" >> $XCE_CONFIG
else
    XLRROOT="$(awk -F'=' '/^Constants.XcalarRootCompletePath/{print $2}' $XCE_CONFIG)"
    if in_container ; then
        grep -q '^Constants.Cgroups' "$XCE_CONFIG" && sed -i 's/^Constants.Cgroups.*/Constants.Cgroups=false/' "$XCE_CONFIG" || echo 'Constants.Cgroups=false' >> "$XCE_CONFIG"
    fi
fi

say "### Setting installation defaults"
#SetXceDefaults.sh -u
PRESETS="XCE_CONFIG XCE_USER XCE_GROUP XCE_HOME XCE_USER_HOME XCE_WORKDIR XCE_LOGDIR XLRDIR XLRGUIDIR XCE_LICENSEDIR XCE_LICENSEFILE XCE_CADDYFILE XCE_HTTP_ROOT XCE_HTTP_PORT XCE_HTTPS_PORT PYTHONHOME XCE_ACCESS_URL XCE_LOGIN_PAGE XCE_CLUSTER_URL CLUSTER_ALIAS XCE_XCALAR_UNIT XCE_USRNODE_UNIT XCE_CGCTRL_CMD"
if [ "$XCE_USER" != "xcalar" ]; then
    PRESETS="$PRESETS XCE_USER"
fi
if [ "$XCE_GROUP" != "xcalar" ]; then
    PRESETS="$PRESETS XCE_GROUP"
fi

if [ -n "$opt_defaults_file" ] && [ -f "$opt_defaults_file" ]; then
    optional_defaults=$(cat $opt_defaults_file)

    for default_line in "${optional_defaults[@]}"; do
        default="$(echo "$default_line" | cut -d '=' -f1)"
        PRESETS="$PRESETS $default"
    done
fi

if [ -n "$upgrade_file" ] && [ -f "$upgrade_file" ]; then
    cp -fp "$upgrade_file" "$XCE_DEFAULT"
fi

IS_UPGRADE=$(test -f "$XCE_DEFAULT"; echo $?)
for ii in $PRESETS; do
    if [ -n "${!ii}" ]; then
        if [ "$IS_UPGRADE" = "0" ]; then
            update_local_config "$ii=${!ii}" "$XCE_DEFAULT"
        else
            echo "$ii=${!ii}" >> "$XCE_DEFAULT"
        fi
    fi
done

say "### Configuring service scripts"
sed -i -e '/PATH=/s#/opt/xcalar#'"$ROOTDIR/opt/xcalar"'#g' "$ROOTDIR"/opt/xcalar/bin/*-service.sh

say "### Updating python and web server config files"
sed --follow-symlinks -i -e "s:root /var/www/xcalar-gui:root ${XCE_HTTP_ROOT}/xcalar-gui:g" "${ROOTDIR}/etc/xcalar/Caddyfile"
sed --follow-symlinks -i -e "s:redirect /assets/htmlFiles/login.html:redirect {\$XCE_ACCESS_URL}:g" "${ROOTDIR}/etc/xcalar/Caddyfile"
sed --follow-symlinks -i "s:except /assets/htmlFiles/login.html:except {\$XCE_LOGIN_PAGE}:g" "${ROOTDIR}/etc/xcalar/Caddyfile"
# fix up the paths for all the xcalar-python36 files
case "$distro" in
    el7|rhel7)
        XCE_PYTHON_SCRIPTS=$(find ${XLRDIR}/bin -type f -exec file {} \;  | sort | awk '{print $0}' | grep -i -e "python script" -e "python3.6 script" | cut -d: -f1)
        ;;
esac
for python_script in $XCE_PYTHON_SCRIPTS; do
    python_script=$(echo "$python_script" | tr -d '\n' | tr -d '\r')
    sed --follow-symlinks -i -e '1 s@^.*$@#!'${XLRDIR}'/bin/python3.6@' "$python_script"
done
sed --follow-symlinks -i -e "s:/opt/xcalar/lib:${XLRDIR}/lib:g" "${XLRDIR}/bin/antlr"

cd ${ROOTDIR}/opt/xcalar/xcalar-gui
if [ -f assets/js/sample-config.js ] && [ ! -f assets/js/config.js ]; then
    say "### Updating GUI config based on hostname"
    cp assets/js/sample-config.js assets/js/config.js
fi
cd -

say "### Updating ldap config files"
LDAP_CONFIG_FILE="${XLRROOT}/config/ldapConfig.json"
if [ -n "$XLRROOT" -a -d "$XLRROOT" ]; then
    if [ -f "$LDAP_CONFIG_FILE" ]; then
        sed --follow-symlinks -i.bak 's/"useTLS" *: *"\([truefals]\{4,\}\)"/"useTLS":\1/g' $LDAP_CONFIG_FILE
        sed --follow-symlinks -i.bak 's/"activeDir" *: *"\([truefals]\{4,\}\)"/"activeDir":\1/g' $LDAP_CONFIG_FILE
    else
        mkdir -p "${XLRROOT}/config"
        echo '{"ldapConfigEnabled": false}' > "$LDAP_CONFIG_FILE"
    fi
    say "### Creating JupyterNotebook folder"
    mkdir -p "${XLRROOT}/jupyterNotebooks"
    chown -R "$XCE_USER:$XCE_GROUP" "${XLRROOT}/jupyterNotebooks"
fi

say "### Setting up Jupyter files"

mkdir -p "$XCE_HOME"/.jupyter
mkdir -p "$XCE_HOME"/.ipython
cp -r ${ROOTDIR}/opt/xcalar/xcalar-gui/assets/jupyter/jupyter/* "${XCE_HOME}/.jupyter"
cp -r ${ROOTDIR}/opt/xcalar/xcalar-gui/assets/jupyter/ipython/* "${XCE_HOME}/.ipython"
chown -R "$XCE_USER:$XCE_GROUP" "$XCE_HOME"/.jupyter "$XCE_HOME"/.ipython

if test -e "${XCE_HOME}/jupyterNotebooks"; then
    echo >&2 "WARNING: Jupyter folder exists but will be moved by upgrade tool."
fi

say "### Fixing test data location"
sed --follow-symlinks -i "s,/netstore/datasets/,${ROOTDIR}/opt/xcalar/test_data/," ${ROOTDIR}/opt/xcalar/xcalar-gui/assets/js/shared/setup.js

say "### Creating empty license file"
touch ${ROOTDIR}/etc/xcalar/XcalarLic.key

#say "### Updating ld cache to pick up newly installed libs"
#sudo ldconfig

say "### Running xcalarctl"
case "$VERSTRING" in
    amzn1|rhel6|el6|ub14)
        cd $XCE_WORKDIR
        xcalarctl "$@"
        ;;
    amzn2|rhel7|el7|ub16)
        sudo systemctl $@ "$XCE_XCALAR_UNIT"
        ;;
    *)
        echo >&2 "Unknown start command for $VERSTRING"
        ;;
esac
