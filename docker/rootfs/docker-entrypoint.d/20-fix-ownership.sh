#!/bin/bash
#
# shellcheck disable=SC2086,SC2015,SC2174

as_user() {
    su-exec $UGID "$@"
}

add_user() {
    local uid="$1" name="$2" home="$3"
    if ! getent passwd $uid >/dev/null; then
        local groups="sudo,docker,disk"
        if [ $uid != 1000 ]; then
            groupadd -o -g $uid $name
            groups="xcalardev,$groups"
        fi
        getent group systemd-journal >/dev/null && groups="$groups,systemd-journal" || true
        getent group adm >/dev/null && groups="$groups,adm" || true
        useradd -o -m -d $home -s /bin/bash -g $uid -G $groups -u $uid $name
        cp -rn /etc/skel/ $home/
        chown -R $uid:$uid $home
    fi
}

fix_user() {
    # Fix groups
    if [ "$CONTAINER_UID" != "" ] && [ $CONTAINER_UID != 0 ]; then
        CONTAINER_USER=${CONTAINER_USER:-xcalardev}
        CONTAINER_HOME="${CONTAINER_HOME:-/home/${CONTAINER_USER}}"
        EXISTING_USER=$(id -un $CONTAINER_UID 2>/dev/null)
        if [ -n "$EXISTING_USER" ] && [ "$EXISTING_USER" == "$CONTAINER_USER" ]; then
            :
        else
            echo >&2 "+ Adding container user $CONTAINER_USER($CONTAINER_UID) HOME=$CONTAINER_HOME"
            groups=sudo,docker,disk

            # Add logging groups
            getent group systemd-journal >/dev/null && groups="$groups,systemd-journal" || true
            getent group adm >/dev/null && groups="$groups,adm" || true

            groupadd --non-unique -g ${CONTAINER_GID} $CONTAINER_USER
            useradd --create-home --home-dir $CONTAINER_HOME --shell /bin/bash --gid $CONTAINER_GID --groups $groups --non-unique --uid $CONTAINER_UID $CONTAINER_USER

        fi
        test -e ${CONTAINER_HOME}/.ssh || mkdir -m 0700 ${CONTAINER_HOME}/.ssh
        if test -w "$CONTAINER_HOME"; then
            find /etc/skel -type f -not -name '.' -not -name '..' -exec cp {} ${CONTAINER_HOME}/ \;
            cat <<-'DEOF' >"$CONTAINER_HOME"/.xcalarinit.sh
			if [ -e "$XLRDIR" ]; then
			    ln -sfn "$XLRDIR" "$HOME"/xcalar
			    ln -sfn "$HOME"/xcalar/.gdbinit "$HOME"/
			    ln -sfn "$HOSTHOME"/.aws "$HOME"/.aws
			    ln -sfn "$HOSTHOME"/.vault-token "$HOME"/.vault-token
			    ln -sfn "$HOSTHOME"/.cshellrc "$HOME"/.cshellrc
			    if [ -f "$XLRDIR"/conf/container.sh ]; then
			        . "$XLRDIR"/conf/container.sh || true
			    fi
			fi
			[ -f "$HOSTHOME"/.cshellrc ] && . "$HOSTHOME"/.cshellrc || true
			[ -f $XLRDIR/.cshellrc ] && . $XLRDIR/.cshellrc
			CONTAINER_CONF_INIT=1
			DEOF
            sed -i '/c2-added$/d' ${CONTAINER_HOME}/.bashrc
            echo '. ~/.xcalarinit.sh # c2-added' >>${CONTAINER_HOME}/.bashrc
            echo 'command -v direnv >/dev/null && eval "$(direnv hook bash)" || true # c2-added' >>${CONTAINER_HOME}/.bashrc
            chown -f $UGID "$CONTAINER_HOME" "$CONTAINER_HOME"/.{ssh,ssh/config,config,cache,m2,ivy2,pip,ccache,jenkins} "$CONTAINER_HOME"/.bash* "$CONTAINER_HOME"/.xcalarinit.sh "$CONTAINER_HOME"/.profile 2>/dev/null
        fi
    fi
}

fix_groups() {
    if [ "$(id -u)" != 0 ]; then
        echo >&2 "ERROR: Must be root to run fixownership"
        return 1
    fi
    if ! getent group 1000 >/dev/null 2>&1; then
        groupadd -g 1000 xcalardev
    fi
    if ! getent group docker >/dev/null; then
        DOCKER_HOST="${DOCKER_HOST:-unix:///var/run/docker.sock}"
        DOCKER_SOCK="${DOCKER_HOST#unix://}"
        if [ -S "$DOCKER_SOCK" ]; then
            if docker_gid=$(stat --format '%g' $DOCKER_SOCK); then
                groupadd -r -o -g $docker_gid docker
            fi
        fi
        getent group docker >/dev/null || groupadd -r -o -g 999 docker
    fi

    if ! getent group sudo >/dev/null; then
        groupadd -r sudo
    fi
    if ! test -d /etc/sudoers.d; then
        mkdir -m 0750 -p /etc/sudoers.d
    fi
    chmod 0750 /etc/sudoers.d

    if ! test -e /etc/sudoers.d/99-sudo; then
        echo '%sudo ALL=(ALL) NOPASSWD:ALL' >/etc/sudoers.d/99-sudo
        chmod 0440 /etc/sudoers.d/99-sudo
    fi
    if ! test -e /etc/sudoers.d/99-allsudo; then
        echo 'ALL ALL=(ALL) NOPASSWD:ALL' >>/etc/sudoers.d/99-allsudo
        chmod 0440 /etc/sudoers.d/99-allsudo
    fi
}

fix_dirs() {
    local ii
    mkdir -p /var/tmp/xcalar-root /mnt/xcalar /mnt/xcalar/pysite /var/opt/xcalar{,/config,/jupyterNotebooks,/stats} /var/opt/xcalar/DataflowStatsHistory{,/configs,/systemStats,/jobStats} /var/log/xcalar /var/tmp/xcalar{,Test} /etc/xcalar /mnt/xcalar{,/jupyterNotebooks,/stats}
    chown $UGID /var/tmp/xcalar-root /mnt/xcalar /mnt/xcalar/pysite /var/opt/xcalar{,/config,/jupyterNotebooks,/stats} /var/opt/xcalar/DataflowStatsHistory{,/configs,/systemStats,/jobStats} /var/log/xcalar /var/tmp/xcalar{,Test} /etc/xcalar /mnt/xcalar{,/jupyterNotebooks,/stats}
    mkdir -p -m 2775 $SHARED_CACHE
    chown $UGID $SHARED_CACHE
    (
        cd $SHARED_CACHE || exit 1
        for ii in sbt ivy2 m2 cache pip ccache jupyter ipython npm jupyter jenkins; do
            as_user mkdir -m 2775 -p $ii
            as_user ln -sfn $PWD/$ii $CONTAINER_HOME/.${ii}
        done
        as_user mkdir -m 2775 -p sbt/boot
    )
    if ! test -e "$SHARED_CACHE"/jupyter/jupyter_notebook_config.py; then
        curl -s http://repo.xcalar.net/deps/jupyter-home.tar.gz | tar zxf - -C $SHARED_CACHE --transform=s,^\.,,
        chown -R $UGID $SHARED_CACHE/{jupyter,ipython}
    fi
    sed -i -r '/^## fixupstart/,/^## fixupend/d' /etc/ssh/ssh_config
    cat <<-EOF | tee -a /etc/ssh/ssh_config >/dev/null
	## fixupstart
	Host *
	    StrictHostKeyChecking no
	    UserKnownHostsFile /dev/null
	## fixupend
	EOF
    local hostuser=${HOSTUSER:-jenkins}
    echo -e 'Host gerrit gerrit.*\n\tUser '$hostuser'\n\n\nHost *\n\tUser '$hostuser'\n\tStrictHostKeyChecking no\n\tUserKnownHostsFile /dev/null\n' >$CONTAINER_HOME/.ssh/config
    chmod 0600 $CONTAINER_HOME/.ssh/config
    chown $UGID $CONTAINER_HOME/.ssh/config
    echo 'umask 003' >/etc/mavenrc
    if test -e /etc/sbt/sbtopts; then
        sed -i -r 's/^#-no-colors/-no-colors/; s@^#-ivy .*$@-ivy '$SHARED_CACHE/ivy2'@; s/^#-mem .*$/-mem 4000/; s@^#-sbt-boot .*$@-sbt-boot '$SHARED_CACHE/sbt/boot'@' /etc/sbt/sbtopts
    fi
    if test -e /opt/xcalar/lib/python3.6/site-packages; then
        echo '/mnt/xcalar/pysite' >/opt/xcalar/lib/python3.6/site-packages/mnt-xcalar-pysite.pth
    fi
}

fix_ownership() {
    fix_groups "$@"
    fix_user "$@"
    fix_dirs "$@"
}

if [ "$(basename -- $0)" == "$(basename -- ${BASH_SOURCE[0]})" ]; then
    if ((XTRACE)); then
        set -x
    fi
    fix_ownership "$@"
fi
