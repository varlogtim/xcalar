#!/bin/bash

# ONE TIME SETUP
# ==============
#
# ***  Download and install [Ubuntu Server 14.04 LTS](http://www.ubuntu.com/download) ***
#     on either bare-metal or a VM you intend to develop with.
#
#   Pick a system name following Xcalar conventions and ensure that
#   it does not conflict with existing names.  The user name you should
#   create during install should match your email ID, i.e., use "jdoe" and
#   not "john" or "jane".
#
#   Please pick a hostname following the theme of 'Great Minds'. This is
#   somewhat open to interpretation. Ask a fellow engineer if the name is taken
#
#   If the system hangs during first post-install boot, there may be a problem
#   with the support for your graphics card.  To fix this, boot into recovery mode.
#   Select 'resume normal boot' from the recovery menu.
#   Edit `/etc/init/plymouth-upstart-bridge.conf`  and append 'sleep 2'.
#   Edit `/etc/default/grub`  and change `GRUB_CMDLINE_LINUX_DEAFULT` to "noplymouth".
#
#   If networking does not come up when the system boots, particularly if
#   ifconfig -a lists a strange device named 'em1', run:
#
# ```
#   sudo apt-get remove biosdevname
#   sudo update-initramfs -u
# ```
#
#   Reboot and boot as normal.
#
# ***  (Optional) Install your preferred Desktop Environment ***
# sudo apt-get install lubuntu-desktop
#
# sudo apt-get install kubuntu-desktop
#
# sudo apt-get install xubuntu-desktop
#
# sudo apt-get install ubuntu-desktop
#
# *** Reboot into desktop ***

# executable portion begins here

# Make sure we're not running inside a python virtualenv
[ ! -z ${VIRTUAL_ENV+_} ] && echo "$0 should not be run from within a python virtualenv. Please run 'xcEnvLeave' and re-run." && exit 1

#
# Set up XCE (XLRDIR) and XD (XLRGUIDIR) repositories
#

usage() {
    say "usage: $0 [-h|--help] [-b]"
    say "-b - build Xcalar to test the configuration"
    say "-h|--help - this help message"
}

unset BUILD

parse_args() {
    while test $# -gt 0; do
        cmd="$1"
        shift
        case "$cmd" in
            --help|-h)
                usage
                exit 1
                ;;
            -b)
                BUILD="1"
                ;;
        esac
    done
}

parse_args "$@"

set -e
if [ -z "$XLRDIR" ]; then
    export XLRDIR="$HOME/xcalar"
fi
if [ -z "$XLRGUIDIR" ]; then
    export XLRGUIDIR="$HOME/xcalar-gui"
fi
if [ -z "$XLRINFRADIR" ]; then
    export XLRINFRADIR="$HOME/xcalar-infra"
fi
if [ -z "$XCE_LICENSEDIR" ]; then
    export XCE_LICENSEDIR="/etc/xcalar"
fi
GITUSER=${GITUSER:-git}
GITHOST="git.int.xcalar.com"
GERRITUSER="${GERRITUSER:-$USER}"
GERRITHOST="gerrit.int.xcalar.com"
XLRPROTO="$GITUSER@$GITHOST:/gitrepos/xcalar-prototype.git"
XLRORIGIN="$GITUSER@$GITHOST:/gitrepos/xcalar.git"
XLRGUI="$GITUSER@$GITHOST:/gitrepos/xcalar-gui.git"
DOCKERPATH="$XLRDIR/Dockerfile"

if [ -f /etc/os-release ]; then
    . /etc/os-release
    case "$ID" in
        rhel|ol)
            ELVERSION="$(echo $VERSION_ID | cut -d'.' -f1)"
            VERSTRING="rhel${ELVERSION}"
            ;;
        amzn)
            IFS=':' read -a CPE <<< "$CPE_NAME"
            if [ "${CPE[*]:2:2}" = "amazon linux" ]; then
                ELVERSION=1
            elif [ "${CPE[*]:4:2}" = "amazon_linux 2" ]; then
                ELVERSION=2
            else
                ELVERSION="$VERSION_ID"
            fi
            VERSTRING="amzn${ELVERSION}"
            ;;
        centos)
            ELVERSION="$(echo $VERSION_ID | cut -d'.' -f1)"
            VERSTRING="el${ELVERSION}"
            ;;
        ubuntu)
            UBVERSION="$(echo $VERSION_ID | cut -d'.' -f1)"
            VERSTRING="ub${UBVERSION}"
            ;;
        *)
            echo >&2 "Unknown OS version: $PRETTY_NAME ($VERSION)"
            ;;
    esac
fi

if [ -z "$VERSTRING" ] && [ -e /etc/redhat-release ]; then
    ELVERSION="$(grep -Eow '([0-9\.]+)' /etc/redhat-release | cut -d'.' -f1)"
    if grep -q 'Red Hat' /etc/redhat-release; then
        VERSTRING="rhel${ELVERSION}"
    elif grep -q CentOS /etc/redhat-release; then
        VERSTRING="el${ELVERSION}"
    else
        cat >&2 /etc/*-release
        die "Unrecognized EL OS version. Please set VERSTRING to rhel6, el6, rhel7, el7, amzn1, or amzn2 manually before running this script"
    fi
fi

add_host_keys () {
    touch ~/.ssh/known_hosts
    chmod 0600 ~/.ssh/known_hosts
    if ! grep -q "$1" ~/.ssh/known_hosts; then
        ssh-keyscan -p "$2" "$1" >> ~/.ssh/known_hosts || true
        ssh-keyscan -p "$2" $(getent hosts $1 | awk '{print $1}') >> ~/.ssh/known_hosts || true
    fi
}

package_install () {
    case "$VERSTRING" in
        ub14|ub16)
            sudo DEBIAN_FRONTEND=noninteractive apt-get install -f -y -qq "$@"
            ;;
        el6|el7|rhel6|rhel7|amzn1|amzn2)
            sudo yum makecache fast && sudo yum install -y "$@"
            ;;
    esac
}

command_exists () {
    command -v "$@" >/dev/null 2>&1
}

# ***  SSH key generation ***
mkdir -p -m 0700 ~/.ssh
chmod 0700 ~/.ssh
if [ ! -e ~/.ssh/id_rsa.pub ]; then
    ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''
fi

if ! command_exists git; then
    package_install git
fi

add_host_keys $GITHOST 22
add_host_keys $GERRITHOST 29418
set +e

ssh -oPort=29418 -o 'PreferredAuthentications=publickey' -o 'StrictHostKeyChecking=no' $GERRITUSER@$GERRITHOST gerrit version
ret=$?
while [ $ret != "0" ]; do
    echo "*** Add ~/.ssh/id_rsa.pub to https://gerrit.int.xcalar.com by logging in with your Google"
    echo "*** account. See http://wiki.int.xcalar.com/mediawiki/index.php/Gerrit for more details."
    echo "~/.ssh/id_rsa.pub:"
    test -e ~/.ssh/id_rsa.pub && cat ~/.ssh/id_rsa.pub || echo "(id_rsa.pub does not exist)"
    echo "*** Once complete, press any key to try again."
    read
    ssh -oPort=29418 -o 'PreferredAuthentications=publickey' -o 'StrictHostKeyChecking=no' $GERRITUSER@$GERRITHOST gerrit version
    ret=$?
    if [ $ret -ne 0 ]; then
        echo >&2 "Couldn't ssh into $GERRITUSER@$GERRITHOST"
        exit $ret
    fi
done
echo "Gerrit access succeeded"

SSH_GIT_ACCESS=1
ssh -o 'PreferredAuthentications=publickey' -o 'StrictHostKeyChecking=no' $GITUSER@$GITHOST "echo" </dev/null
ret=$?
while [ $ret != "0" ]; do
    echo "*** If you wish to access the xcalar prototype and xcalar-gui origin git repositories,"
    echo "*** send ~/.ssh/id_rsa.pub to Darrin(dwillis), Ted(thaining), or Amit (abakshi) for"
    echo "*** repo access, wait for a response, and press Enter."
    echo "*** Otherwise, press Enter and these git remotes will not be installed."
    read
    ssh -o 'PreferredAuthentications=publickey' -o 'StrictHostKeyChecking=no' $GITUSER@$GITHOST "echo"
    ret=$?
    if [ $ret -ne 0 ]; then
        echo >&2 "Couldn't ssh into $GITUSER@$GITHOST"
        echo >&2 "Not adding prototype remote to xcalar, origin remote to xcalar-gui"
        SSH_GIT_ACCESS=0
        break;
    fi
done
test "$SSH_GIT_ACCESS" = "1" && echo "Git access succeeded"
set -e

# ***  Clone the gerrit Repository as the primary remote gerrit ***
if [ ! -e "$XLRDIR" ]; then
    git clone --recurse-submodules -o "gerrit" -b "trunk" ssh://$GERRITUSER@${GERRITHOST}:29418/xcalar.git "$XLRDIR"
else
    echo "xcalar already exists at $XLRDIR"
fi
if [ "$SSH_GIT_ACCESS" = "1" ]; then
    cd "$XLRDIR"
    if ! git remote -v | grep -q prototype; then
        git remote add prototype "$XLRPROTO"
    fi
    git remote update
    cd -
fi

# *** Clone the gerrit Repository as the primary remote gerrit ***
if [ ! -e "$XLRGUIDIR" ]; then
    git clone --recurse-submodules -o "gerrit" -b "trunk" ssh://$GERRITUSER@${GERRITHOST}:29418/xcalar/xcalar-gui.git "$XLRGUIDIR"
else
    echo "xcalar gui already exists at $XLRGUIDIR"
fi
if [ "$SSH_GIT_ACCESS" = "1" ]; then
    cd "$XLRGUIDIR"
    if ! git remote -v | grep -q origin; then
        git remote add origin "$XLRGUI"
    fi
    git remote update
    cd -
fi

# *** Clone the gerrit Repository as the primary remote gerrit ***
if [ ! -e "$XLRINFRADIR" ]; then
    git clone --recurse-submodules -o "gerrit" -b "master" ssh://$GERRITUSER@${GERRITHOST}:29418/xcalar/xcalar-infra.git "$XLRINFRADIR"
else
    echo "xcalar infra already exists at $XLRINFRADIR"
fi
if [ "$SSH_GIT_ACCESS" = "1" ]; then
    cd "$XLRINFRADIR"
    git remote update
    cd -
fi

#
# Set up git review
#

PIP_CMD="pip"
if ! command_exists pip && command_exists /opt/xcalar/bin/python3; then
    PIP_CMD="/opt/xcalar/bin/python3 -m pip"
fi

if ! command_exists git-review; then
    sudo -H $PIP_CMD install -U git-review
    for dir in "$XLRDIR" "$XLRGUIDIR"; do
        cd "$dir"
        git review -s
        git fetch gerrit
        cd -
    done
fi

#
# Create the base environment
#

. $XLRDIR/doc/env/xc_aliases


# remove older config from .bashrc
sed -i -e '/xcEnvEnter/d' ~/.bashrc

rm -f ~/.xcalar_profile

cat > ~/.xcalar_profile <<EOF
export XLRDIR="\${XLRDIR:-$XLRDIR}"
export XLRGUIDIR="\${XLRGUIDIR:-$XLRGUIDIR}"
export XCE_LICENSEDIR="\${XCE_LICENSEDIR:-$XCE_LICENSEDIR}"

. \$XLRDIR/doc/env/xc_aliases

xcEnvEnter
EOF

# ***  Source xc_aliases in your ~/.bashrc ***
if ! grep -q 'xcalar_profile' ~/.bashrc; then
    echo ". ~/.xcalar_profile" >> ~/.bashrc
fi

# set up jupyter
test -d ~/.jupyter && rm -rf ~/.jupyter
ln -sfn $XLRGUIDIR/assets/jupyter/jupyter ~/.jupyter
test -d ~/.ipython && rm -rf ~/.ipython
ln -sfn $XLRGUIDIR/assets/jupyter/ipython ~/.ipython
mkdir -p ~/jupyterNotebooks

#
# Setup resources needed to build sanity
#

# Set up default xcalar working directories
sudo mkdir -p "/var/opt/xcalar"
sudo mkdir -p "/var/opt/xcalarTest"
sudo mkdir -p "/var/log/xcalar"
sudo chown $USER "/var/opt/xcalar"
sudo chown $USER "/var/opt/xcalarTest"
sudo chown $USER "/var/log/xcalar"

# Configure the system to use the test license
sudo mkdir -p $XCE_LICENSEDIR
sudo ln -sfn $XLRDIR/src/data/EcdsaPub.key $XCE_LICENSEDIR
sudo ln -sfn $XLRDIR/src/data/XcalarLic.key $XCE_LICENSEDIR

# Put the latest versions of system config files in place
sudo install -o root -g root -m 644 "$XLRDIR/conf/xcalar-limits.conf" "/etc/security/limits.d/90-xclimits.conf"
sudo rm -f "/etc/logrotate.d/Xcalar" "/etc/logrotate.d/xcalar"
sudo install -o root -g root -m 644 "$XLRDIR/conf/xcalar-logrotate.conf" "/etc/logrotate.d/xclogrotate"

sudo mkdir -p /var/www
test -r /var/www/xcalar-gui && sudo rm /var/www/xcalar-gui
sudo ln -sfn $XLRGUIDIR/xcalar-gui /var/www/xcalar-gui

# This is needed because if $HOME is 0700, and xcalar-gui is under $HOME/, then caddy can't read those files.
chmod 0755 $HOME

#
# Make front and back ends
#

# Running 'make trunk' in xcalar-gui, requires a build in xcalar

xcEnvEnter
xclean
set -e
if [ "$BUILD" = "1" ]; then
    cd $XLRDIR
    bin/cmBuild clean
    bin/cmBuild config prod
    bin/cmBuild
    cd -

    cd $XLRGUIDIR
    git clean -fxd
    git reset --hard HEAD
    make trunk
    cd -
fi

#
# Build sanity
#
if [ "$BUILD" = "1" ]; then
    cd $XLRDIR
    cmBuild sanity
    cd -
fi
set +e
xcEnvLeave

#
# Do git config
#
git config --global include.path "$XLRDIR/doc/env/dot_gitconfig"

if [ "$(git config user.name)" == "<YOUR NAME>" ]; then
    echo >&2 "!! Please fix your git name! Run 'git config --global user.name \"First Last\"'"
fi
if [ "$(git config user.email)" == "<YOUR NAME>" ]; then
    echo >&2 "!! Please fix your git email! Run 'git config --global user.email \"yourname@xcalar.com\"'"
fi

#
# Enable access to USB sticks and removable media via group disk
#
if ! grep "disk" /etc/group | grep -q "$USER"; then
    sudo usermod -aG disk "$USER"
fi

optXc="/opt/xcalar"
sudo ln -sfn "$XLRDIR/scripts" "$optXc"

#
# Setup gdb scripting
#
if [ -a ~/.gdbinit ] && ! diff ~/.gdbinit "$XLRDIR/.gdbinit"; then
    echo "Your personal ~/.gdbinit is different from ~/xcalar.gdbinit"
    echo "Please save your changes before continuing,"
    echo "$HOME/.gdbinit will be replaced by a symlink to $XLRDIR/.gdbinit"
    echo
    echo "You can undo this change and add the following to your"
    echo "custom ~/.gdbinit:"
    echo
    echo"    py import os; XLRDIR=os.getenv('XLRDIR'); gdb.execute('source %s/.gdbinit' % XLRDIR)"
    echo
    echo "Press Enter to continue."
    read
fi

cd $HOME && rm -f ~/.gdbinit && ln -s "$XLRDIR/.gdbinit" .gdbinit

jail=/opt/xcalar/jail_py/usr
if grep -q $jail /etc/fstab; then
    sudo sed -i -e '\@'$jail'@d' /etc/fstab
fi
if grep -q $jail /etc/rc.local; then
    sudo sed -i -e '\@'$jail'@d' /etc/rc.local
fi

for user in xlrlowpriv xlrlowprivnet; do
    if getent passwd $user &>/dev/null; then
        sudo userdel -f -r $user || true
    fi
done

# ***  reboot ***
echo "Please reboot to make sure all changes are correctly applied"
