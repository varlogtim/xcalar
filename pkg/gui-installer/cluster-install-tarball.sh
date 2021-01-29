#!/bin/bash
# Copyright 2015-2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

echo "$0 $@" >> ${TMPDIR:-/tmp}/cluster-install.log

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
. "$DIR/installer-sh-lib"

export PATH=$DIR:${XCE_INSTALLER_ROOT}/bin:$PATH

remote_path="PATH=/usr/sbin:/usr/bin:/sbin:/bin:\$PATH"

xce_root_path='/mnt/xcalar'

XCE_USER="xcalar"
XCE_GROUP="xcalar"

is_upgrade=0
installer_regex='xcalar-*-userinstaller'

install_dir=""
upgrade_in_progress="0"

cleanup () {
    nstep=0
    tsteps=0

    if [ "$upgrade_in_progress" == "1" ]; then
        tsteps=1
        _step "Cleaning up upgrade in progress"
        _pssh "cd $install_dir && test -h .backup && rm -rf etc opt SHA1SUM var && mv .backup/* . && rm -rf .backup cleanup_backup_dir"
    fi
}

cluster_config() {
    test_file="${TMPDIR}/id_value"
    dest_host="${hosts_array[0]}"
    nstep=0
    local tsteps
    if [ "$is_upgrade" == "1" ]; then
        tsteps=18
    else
        tsteps=17
    fi

    if [ -z "$install_dir" ]; then
        die $nstep "An installation directory is not set"
    fi

    # check if we can contact the hosts
    _step "Check host connection"
    _pssh ls -l || die $nstep "Some hosts unreachable"
    if test "$NO_PING" == "0" && test "${hosts_array[0]}" != "${priv_hosts_array[0]}"; then
        for host in $(cat $priv_hosts_file); do
            host=$(echo "$host" | tr -d '\n' | tr -d '\r')
            _ssh $dest_host "ping -c 3 $host" || die $nstep "Some hosts unreachable on private network"
        done
    fi

    # verify that the RPM version of Xcalar is not installed
    _step "Check for colliding Xcalar installs"
    for host in "${hosts_array[@]}"; do
        if _ssh_ignore_failure $host "test -f /etc/default/xcalar"; then
            die $nstep "/etc/default/xcalar file exists on one or more nodes, indicating that an incompatable Xcalar install may exist.  Uninstall Xcalar and delete the file to avoid problems running this version of Xcalar."
        fi
    done

    # three steps to check uid, gid, group names
    check_user_identity

    # three steps to check directories used in /tmp
    check_tmpdir

    # verify that xce_root_path and install_dir exist
    _step "Verify $xce_root_path and $install_dir"
    if [ "$nfs_mode" == "reuse" ]; then
        _pssh ls -l $xce_root_path || die $nstep "XcalarRoot on some hosts does not exist"
        _pssh touch ${xce_root_path}/write-test || die $nstep "Xcalar root on some hosts not writeable"
        _pssh rm -f ${xce_root_path}/write-test
    fi
    if [ "$pre_config" == "1" ]; then
        _pssh $sudo mkdir -p "$install_dir" || die $nstep "Unable to create installation directory $install_dir"
        _pssh $sudo chown "$XCE_USER:$XCE_GROUP" "$install_dir" || die $nstep "Unable to set owner of $install_dir to $XCE_USER:$XCE_GROUP"
        _pssh chmod 755 "$install_dir" || die $nstep "Unable to set permission of $install_dir to 755"
    else
        _pssh ls -l $install_dir || die $nstep "Installation directory on some hosts does not exist"
        _pssh touch ${install_dir}/write-test || die $nstep "Installation directory on some hosts not writeable"
        _pssh rm -f ${install_dir}/write-test
    fi
    if [ -n "$serdes_dir" ]; then
        _pssh ls -l $serdes_dir || die $nstep "Serialize/de-serialize directory on some hosts does not exist"
        _pssh touch ${serdes_dir}/write-test || die $nstep "Serialize/de-serialize directory on some hosts not writeable"
        _pssh rm -f ${serdes_dir}/write-test
    fi

    # one step to create cluster config
    create_cluster_config_file "${install_dir}/var/log/xcalar"

    _step "Create tmpdir /tmp/$user"
    _pssh "mkdir -p -m 0700 /tmp/$user" || die $nstep "Failed to create tmpdir /tmp/$user"

    _step "Create list of installed site packages"
    _pssh "if test -f ${XLRDIR}/bin/get-pip.py; then $remote_path ${XLRDIR}/bin/python3.6 -m pip freeze | grep -v xcalar > /tmp/$user/requirements.txt; fi"
    _pssh "if test -f ${XLRDIR}/bin/pip3.6; then $remote_path ${XLRDIR}/bin/pip3.6 freeze | grep -v xcalar > /tmp/$user/requirements.txt; fi"

    _step "Record any enabled XD extenstions"
    gui_ext_enabled="${XLRDIR}/xcalar-gui/assets/extensions/ext-enabled"
    _pssh "if test -d $gui_ext_enabled; then cd $gui_ext_enabled && ls -A > /tmp/$user/xd-ext-enabled.txt; fi"

    _step "Get cluster version (upgrade only)"
    if [ "$is_upgrade" == "1" ]; then
        _scp "${dest_host}:${install_dir}/etc/xcalar/VERSION" "${TMPDIR}/VERSION" || die $nstep "Unable to get cluster version"
    fi

    _step "Cleaning up a previous install"
    if [ "$is_upgrade" == "1" ]; then
        # if we are upgrading from a non-systemd installation, definitely stop the cluster
        _pssh "if ! test -f ${XLRDIR}/bin/usrnode-service.sh; then $remote_path ${XLRDIR}/bin/xcalarctl stop-supervisor; fi"
        _pssh "if systemctl cat xcalar-services.target >/dev/null 2>&1; then $remote_path sudo systemctl stop xcalar-services.target; fi"
    fi
    _pssh "if systemctl cat xcalar.service >/dev/null 2>&1; then $remote_path sudo systemctl stop xcalar.service; fi"
    if [ -n "$XCE_XCALAR_UNIT" ]; then
        _pssh "if systemctl cat $XCE_XCALAR_UNIT >/dev/null 2>&1; then $remote_path sudo systemctl stop $XCE_XCALAR_UNIT; fi"
    fi
    backup_dir="${install_dir}/backup.$(date +'%s')"
    _pssh "mkdir -p ${backup_dir}" || die $nstep "unable to create backup dir"
    _pssh "if test -e ${install_dir}/etc; then mv ${install_dir}/etc $backup_dir; fi"
    _pssh "if test -e ${install_dir}/opt; then mv ${install_dir}/opt $backup_dir; fi"
    _pssh "if test -e ${install_dir}/var; then mv ${install_dir}/var $backup_dir; fi"
    _pssh "if test -e ${install_dir}/SHA1SUM; then mv ${install_dir}/SHA1SUM $backup_dir; fi"
    _pssh "rm -f ${install_dir}/xcalar\* || true"
    _pssh "rmdir ${backup_dir} > /dev/null || true"
    if [ "$is_upgrade" == "1" ]; then
        _pssh "if test -e \"${backup_dir}\"; then ln -s $backup_dir ${install_dir}/.backup; fi"
        upgrade_in_progress="1"
        cleanup_backup_dir="$backup_dir"
    fi

    _step "Create destination folders"
    _pssh mkdir -p ${install_dir}/etc/xcalar || die $nstep "Failed to create ${install_dir}/etc/xcalar"

    _step "Copy cluster config"
    backup_config="${backup_dir}/etc/xcalar/default.cfg"
    target_config="${install_dir}/etc/xcalar/default.cfg"
    _pscp $TMPDIR/default.cfg "${target_config}" || die $nstep "Failed to upload default.cfg to ${install_dir}/etc/xcalar"
    if [ "$is_upgrade" == "1" ]; then
        _step "Deploy cluster config from backup"
        _pssh "if test -e ${backup_config}; then cp -p ${backup_config} ${target_config}; fi"
    fi
}

upgrade_config() {
    dest_host="${hosts_array[0]}"
    data_sep="#%@"
    nstep=0
    tsteps=3

    _step "Checking that ${install_dir}/etc/default/xcalar exists"
    _pssh ls -l ${install_dir}/etc/default/xcalar || die $nstep "Xcalar not properly installed -- /etc/default/xcalar not found"

    _step "Getting XCE_USER"
    OUTDIR="${TMPDIR}/${nstep}/0"
    _ssh $dest_host ". ${install_dir}/etc/default/xcalar; XCE_USER=\${XCE_USER:-xcalar}; echo \\$data_sep\$XCE_USER"
    XCE_USER=$(cat "$OUTDIR/stdout" | awk -F "$data_sep" '{ print $2 }' | tr -d '\n' | tr -d '\r')

    _step "Getting XCE_GROUP"
    OUTDIR="${TMPDIR}/${nstep}/0"
    _ssh $dest_host ". ${install_dir}/etc/default/xcalar; XCE_GROUP=\${XCE_GROUP:-xcalar}; echo \\$data_sep\$XCE_GROUP"
    XCE_GROUP=$(cat "$OUTDIR/stdout" | awk -F "$data_sep" '{ print $2 }' | tr -d '\n' | tr -d '\r')
}

deploy_installer () {
    nstep=0
    tsteps=1

    _step "Uploading installer to ${install_dir}"
    _pscp $installer_path ${install_dir} || die $nstep "Failed to upload installer"
}

deploy_shared_config () {
    nstep=0
    tsteps=3

    dest_host="${hosts_array[0]}"
    _step "Creating the shared config directory on $dest_host"
    _ssh $dest_host mkdir -p "$xce_root_path/config" || die $nstep "Unable to create shared config directory on $dest_host"

    _step "Creating JWT secret"
    if ! [ -f ${XCE_INSTALLER_ROOT}/config/authSecret ]; then
        python ${DIR}/stringSeed.py > ${XCE_INSTALLER_ROOT}/config/authSecret
        chmod 600 ${XCE_INSTALLER_ROOT}/config/authSecret
    fi

    _step "Copying config files to the shared directory"
    _ssh $dest_host test -w "$xce_root_path/config" || die $nstep "Unable to write to shared config directory on $dest_host"
    _scp -rp ${XCE_INSTALLER_ROOT}/config/* "$dest_host:$xce_root_path/config" || die $nstep "Copy to shared config directory on $dest_host failed"
}

xcalar_install () {
    nstep=0
    local tsteps
    if [ "$is_upgrade" == "1" ]; then
        tsteps=8
    else
        tsteps=5
    fi

    if [ "$pre_config" == "1" ]; then
        tsteps=9

        _step "Create pre-config setup file"
        echo "XCE_USER=$XCE_USER" > $TMPDIR/setup.txt
        echo "XCE_GROUP=$XCE_GROUP" >> $TMPDIR/setup.txt
        echo "XCE_HOME=/home/${XCE_USER}" >> $TMPDIR/setup.txt
        echo "XCE_INSTALLDIR=${install_dir}" >> $TMPDIR/setup.txt
        echo "XCE_FIREWALL_CONFIG=1" >> $TMPDIR/setup.txt
        echo "XLRROOT=${xce_root_path}" >> $TMPDIR/setup.txt
        _pscp $TMPDIR/setup.txt "/tmp/$user/setup.txt" || die $nstep "Unable to copy pre-config setup file to cluster"

        _step "Running preconfig"
        _pssh "/usr/bin/env bash -exec \"cd ${install_dir}; ${remote_path} ./$installer_name -s -f /tmp/$user/setup.txt > /tmp/$user/pre-config.log 2>&1\""
        pcfg_rc=$?

        _step "Verifying config"
        _pssh "/usr/bin/env bash -exec \"cd ${install_dir}; ${remote_path} ./$installer_name -ve -f /tmp/$user/setup.txt > /tmp/$user/verify.log 2>&1\""

        file_idx=0
        for host in "${hosts_array[@]}"; do
            _scp "${host}:/tmp/$user/pre-config.log" "${TMPDIR}/pre_config.log.${file_idx}"
            _scp "${host}:/tmp/$user/verify.log" "${TMPDIR}/verify.log.${file_idx}"
            cat "${TMPDIR}/verify.log.${file_idx}"
            file_idx=$(( $file_idx + 1 ))
        done
        echo "#" && echo "#" && echo "#"

        [ $pcfg_rc -ne 0 ] && die $nstep "Error during cluster pre-config -- pre-config and verify logs can be found $TMPDIR or with other installer logs after installer exit"
    fi

    _step "Deploy license"
    _pscp "$license_file" ${install_dir}/etc/xcalar/XcalarLic.key || die $nstep "Failed to deploy license"

    _step "Install Xcalar"
    start_date="$(date)"
    if [ -n "$XCE_CLUSTER_HOST" ]; then cluster_alias="CLUSTER_ALIAS=1"; fi
    if [ -f "${XCE_INSTALLER_ROOT}/config/xcalar.defaults" ]; then
        old_cluster_alias="$(. ${XCE_INSTALLER_ROOT}/config/xcalar.defaults && echo $CLUSTER_ALIAS)"
        old_cluster_url="$(. ${XCE_INSTALLER_ROOT}/config/xcalar.defaults && echo $XCE_CLUSTER_URL)"

        if [ "$old_cluster_alias" = "1" ]; then cluster_alias="CLUSTER_ALIAS=1"; fi
        if [ -n "$old_cluster_url" ]; then cluster_url="XCE_CLUSTER_URL=${old_cluster_url}"; fi
    fi
    cluster_url="${cluster_url:-XCE_CLUSTER_URL=https://${XCE_CLUSTER_HOST:-${hosts_array[0]}}:8443/assets/htmlFiles/login.html}"
    identity_str="XCE_USER=${XCE_USER} XCE_GROUP=${XCE_GROUP}"

    upgrade_config_file=""
    if [ "$is_upgrade" == "1" ]; then
        upgrade_config_file="-u ${backup_dir}/etc/default/xcalar"
    fi

    optional_defaults="XCE_XCALAR_UNIT XCE_USRNODE_UNIT XCE_CGCTRL_CMD XCE_ASUP_TMPDIR SYSTEMD_UNIT_DIR"
    opt_defs_file=""
    for opt_def in $optional_defaults; do
        if [ -n "${!opt_def}" ]; then
            echo "${opt_def}=${!opt_def}" >> "${TMPDIR}/opt_defs.txt"
            if [ -z "$opt_defs_file" ]; then
                opt_defs_file="-o /tmp/${user}/opt_defs.txt"
            fi
        fi
    done

    _pssh "rm -f /tmp/${user}/opt_defs.txt"
    if [ -n "$opt_defs_file" ]; then
        _pscp "${TMPDIR}/opt_defs.txt" "/tmp/${user}"
    fi

    _pssh "/usr/bin/env bash -exec \"cd ${install_dir}; ${remote_path} ${cluster_url} ${cluster_alias} ${identity_str} ./$installer_name -d $install_dir -- ${upgrade_config_file} ${opt_defs_file} stop > /tmp/$user/installer.log 2>&1\""
    install_rc=$?

    file_idx=0
    echo -e "#-----\n# starting install\n# ${start_date}\n#-----" >> "${TMPDIR}/installer-combined.log"
    for host in "${hosts_array[@]}"; do
        host_log="${TMPDIR}/installer.log.${file_idx}"
        _scp "${host}:/tmp/$user/installer.log" "$host_log"
        echo -e "\n#\n# ${host}\n#\n" >> "${TMPDIR}/installer-combined.log"
        [ -e "$host_log" ] && cat "$host_log" >> "${TMPDIR}/installer-combined.log"
        file_idx=$(( $file_idx + 1 ))
    done

    test "$install_rc" = "0" || die $nstep "Failed to install Xcalar package"

    _step "Install Python Site Packages"
    _pscp "${DIR}/pip-update.sh" "/tmp/${user}"
    _pssh "if test -f /tmp/${user}/requirements.txt; then ${remote_path} /tmp/${user}/pip-update.sh /tmp/${user}/requirements.txt $XLRDIR $user; fi"

    _step "Re-enable any XD extensions"
    ext_enabled_file="/tmp/$user/xd-ext-enabled.txt"
    _pscp "${DIR}/xd-ext-copy.sh" "/tmp/${user}"
    _pssh "if test -f $ext_enabled_file; then /usr/bin/env bash -exec \"/tmp/${user}/xd-ext-copy.sh ${install_dir} ${ext_enabled_file} >> /tmp/${user}/xd-ext-copy.log 2>&1\"; fi"
    _pssh "rm -f $ext_enabled_file"

    file_idx=0
    for host in "${hosts_array[@]}"; do
        _scp "${host}:/tmp/$user/pip-install.log" "${TMPDIR}/pip-install.log.${file_idx}"
        _scp "${host}:/tmp/$user/xd-ext-copy.log" "${TMPDIR}/xd-ext-copy.log.${file_idx}"
        file_idx=$(( $file_idx + 1 ))
    done

    if [ "$is_upgrade" == "1" ]; then
        _step "Preserve previous Caddyfile's tls derivative."
        # TODO: this is broken -- it's doing work on the installer host, not the cluster hosts
        # a new Caddyfile was set during install step;
        # preserve tls line from backed up previous Caddyfile (if any) into this new one
        # caddy_backup="${backup_dir}/etc/xcalar/Caddyfile"
        # caddy_target="${install_dir}/etc/xcalar/Caddyfile"
        # if [ -f "$caddy_backup" ]; then
        #    preserve_previous_caddy_configuration "$caddy_backup" "$caddy_target"
        # fi

        _step "Delete existing authenticated sessions"
        cluster_host="${hosts_array[0]}"
        xcalar_root="$(cat ${XCE_INSTALLER_ROOT}/tmp/clusterXcalarRoot)"
        _ssh "$cluster_host" "rm -rf $xcalar_root/auth/*"
    fi
}

cluster_start () {
    nstep=0
    tsteps=1
    startup_cmd="start"

    if [ "$is_upgrade" == "1" ]; then
        vsort_file="${TMPDIR}/version_sort"
        cluster_version="$(cat "${TMPDIR}/VERSION" | grep xcalar | cut -d- -f 2)"
        echo "2.0.0" > "$vsort_file"
        echo "$cluster_version" >> "$vsort_file"
        older_version="$(cat "$vsort_file" | sort -V | head -1)"
        [ "$older_version" != "2.0.0" ] && startup_cmd="start-supervisor"
        _pssh "rm -f ${install_dir}/.backup"
        upgrade_in_progress="0"
    fi

    _step "Start Xcalar"
    xcalar_service="xcalar.service"
    if [ -n "$XCE_XCALAR_UNIT" ]; then
        xcalar_service="$XCE_XCALAR_UNIT"
    fi
    _pssh "${remote_path} sudo systemctl start $xcalar_service" || die $nstep "Failed to start xcalar"
}

cluster_stop () {
    nstep=0
    tsteps=1

    _step "Stop Xcalar"
    _pssh "$remote_path sudo systemctl stop xcalar.service"
}

check_install_flags () {
    if [ -z "$install_dir" ]; then
        die 1 "--install-dir was not specified"
    fi
    if [ "$pre_config" == "0" ]; then
        if [ "$nfs_mode" != "reuse" ]; then
            [ -z "$nfs_host" ] && nfs_host="(empty)"
            [ -z "$nfs_folder" ] && nfs_folder="(emtpy)"
            [ -z "$nfs_dest" ] && nfs_dest="(empty)"
            die 1 "pre-config is not set and NFS flags are incompatible: --nfs-reuse: $nfs_dest, --nfs_host: $nfs_host, --nfs_folder: $nfs_folder"
        fi

        if [ "$ldap_mode" != "external" ]; then
            [ -z "$ldap_domain" ] && ldap_domain="(empty)"
            [ -z "$ldap_password" ] && ldap_password="(empty)"
            [ -z "$ldap_org" ] && ldap_org="(empty)"
            die 1 "unknown comibination of LDAP flags: --ldap-domain: $ldap_domain, --ldap_password: $ldap_password, --ldap_org: $ldap_org"
        fi
    fi
}

delete_cluster_config () {
    # delete any previously created/discovered cluster config
    rm -rf "${XCE_INSTALLER_ROOT}"/config/*
}

upgrade_license () {
    # a bit of a hack, in case we're called with a bad license file
    config_dir="${XCE_INSTALLER_ROOT}/config"
    local license_contents=""
    [ -f $license_file ] && license_contents="$(cat "$license_file")"
    if [ -z $license_file ] || ! [ -f $license_file ] || [ -z "$license_contents" ]; then
        [ -f "${config_dir}/XcalarLic.key" ] && license_file="${config_dir}/XcalarLic.key"
    fi
}

cluster_install () {
    check_install_flags
    license_check
    cluster_config
    deploy_installer
    cluster_stop
    xcalar_install
    nfs_setup
    nfs_write_test
    ldap_install "$XLRDIR"
    create_default_user
    setup_hot_patches
    deploy_shared_config
    cluster_start
    delete_cluster_config
}

cluster_upgrade () {
    is_upgrade="1"
    check_install_flags
    upgrade_license
    license_check
    cluster_config
    deploy_installer
    cluster_stop
    xcalar_install
    copy_xcalar_root "$XCE_CONFIG"
    nfs_setup
    nfs_write_test
    upgrade_shared_config "$XCE_CONFIG"
    cluster_start
    delete_cluster_config
}

cluster_uninstall () {
    nstep=0
    tsteps=4
    cluster_config_dir="${TMPDIR}/cluster_config"
    dest_host="${hosts_array[0]}"

    _step "Check host connection"
    _pssh ls -l || die $nstep "Some hosts unreachable"

    _step "Stop Xcalar"
    _pssh "$remote_path sudo systemctl stop xcalar.service" || die $nstep "Failed to stop xcalar services"

    _step "Remove Xcalar"
    _pssh "rm -rf ${install_dir}/*" || die $nstep "Failed to remove xcalar"

    delete_cluster_config
}

cluster_discover () {
    nstep=0
    tsteps=1

    _step "Get cluster config"
    get_cluster_config "$XCE_CONFIG" "${XCE_LICENSEDIR}/${XCE_LIC_FILE}" "$XCE_DEFAULTS"
}

parse_args "$@"

if [ $nfs_reuse -eq 1 ]; then
    xce_root_path="$nfs_dest"
fi

XCE_CONFIG="${install_dir}/etc/xcalar/default.cfg"
XCE_DEFAULTS="${install_dir}/etc/default/xcalar"
XCE_LICENSEDIR="${install_dir}/etc/xcalar"
XCE_LIC_FILE="XcalarLic.key"
XLRDIR="${install_dir}/opt/xcalar"

if [ -z "$script" ]; then
    # eg, 01-license-check.sh -> 01-license-check
    script="$(basename $0 .sh)"
    # eg, 01-license-check -> license_check
    script="$(echo ${script##[0-9][0-9]-} | tr - _)"
fi

if [ "$(type -t $script)" = function ]; then
    eval $script
else
    die 1 "Unknown script $script"
fi
