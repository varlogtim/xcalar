#!/bin/bash

# Tests installer within an ubuntu/centos container. This script runs inside
# the container. It executes the installer, configures the installation,
# and runs basic checks.

XLRDIR=/opt/xcalar

# Determine OS.
centos7Check=`fgrep 'CentOS Linux release 7' /etc/system-release 2> /dev/null`
ubuntuCheck=`fgrep 'Ubuntu' /etc/os-release 2> /dev/null`
if [ ! -z "$centos7Check" ]; then
    OS="CENTOS7"
elif [ ! -x "$ubuntuCheck" ]; then
    OS="UBUNTU"
else
    echo "Unknown OS"
    exit 1
fi

# Since this container goes away after the test terminates, this function grabs
# all potentially useful diagnostic files before exiting.
function abort()
{
    trap 'exit 1' ERR

    local diagsDirName="installerTest$(date +"%s")"
    local diagsDir="/xcalar/${diagsDirName}/"
    mkdir $diagsDir

    trap '' ERR
    cp ./core.usrnode* $diagsDir &> /dev/null
    cp ./core.xccli* $diagsDir &> /dev/null
    cp /var/log/xcalar/* $diagsDir &> /dev/null
    if [ "$OS" = "UBUNTU" ]; then
        dpkg -l &> $diagsDir/packages.txt    # Grab dpkg state.
    fi
    # The below will list any unresolved dynamically linked libs for usrnode.
    ldd $XLRDIR/bin/usrnode &> $diagsDir/lddUsrnode.txt

    echo "Installer test failed. See console output and "
    echo "'\$XLRDIR/${diagsDirName}'."

    exit 1
}

trap 'abort' ERR

# This script is not intelligent if there are multiple installers.
installer=`find /xcalar/build -name "xcalar-*-installer" | head -1`
$installer --noStart

cat > /etc/xcalar/default.cfg <<EOF
Constants.EnforceVALimit=false
Constants.BufferCacheLazyMemLocking=true
$(/opt/xcalar/scripts/genConfig.sh /etc/xcalar/template.cfg - $(hostname))
EOF

service xcalar start

UsrnodeTimeoutSecs=30
timeoutTime=$(( $(date +%s) + $UsrnodeTimeoutSecs ))
echo "timeout at $timeoutTime"

gotVersion=false
while [ "$gotVersion" = "false" ]; do
    sleep 2 # Wait for usrnodes to start up.
    version="$(${XLRDIR}/bin/xccli -c "version")"
    echo "$version"
    if [[ "$version" =~ "Backend Xcalar API Version Signature" ]]; then
        gotVersion=true
    else
        if [ $(date +%s) -ge $timeoutTime ]; then
            echo "xccli failed to fetch version from usrnode"
            abort
        fi
    fi
done

support="$(${XLRDIR}/bin/xccli -c "support --generate")"
echo "$support"

if [ `ls -1 /var/opt/xcalar/support | wc -l` != 1 ]; then
    echo "Failed to generate support files."
    abort
fi
