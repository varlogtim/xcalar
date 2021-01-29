#!/bin/bash

### README
# This script creates a container, installs an installer to it and then runs the
# upload stats script to publish stats tables. It exposes both the XD and JDBC
# ports to the host (by default on 9443 and 20000 respectively)

# You will need to be auth'ed with vault as it needs to retrieve a secret for the
# license number, otherwise imports/exports won't work

# To set up the container it creates a script on the fly with a HEREDOC that gets
# exported into docker at /statsmart.sh

# The use case of a local checkout is handled by creating an installer on the fly
# as it's fast and safer than running xcalar with xc2

## NB
# you cannot have two instances of this running without changing port numbers
# otherwise you end up with a conflict/port already assigned error
#
# also any attempt to run another cluster on the host using xc2 will mess stuff up
# as the contenerized processes are visible to the host and xc2 is proc based.

### TODO
# - It'd be nice to set the name of the container to "statsmart", but need to modify crun first
# - override port assignment from host's env var if available
# - check if port is already in use
# - make it work with local checkout, just create an installer on the fly

usage() {
    cat << EOF
$0:

starts a docker container, installs xcalar into it and loads stats at path. Must be passed at min
two arguments. If no further opotions for upload_data.py are passed, it assumes all data should be
loaded

Usage:

    [arguments]
    path to installer
    path to stats directory to load

    remainder of the arguments will be treated as options to upload_data.py

EOF
}

if [ $# -lt 2 ]; then
    usage
    exit 1
fi

INSTALLER_PATH=$1
STATS_DATA_PATH=$2
shift; shift

if [ ! -f "${INSTALLER_PATH}" ] ; then
    echo "Invalid installer: ${INSTALLER_PATH} does not exist or is not a file"
    exit 2
fi

if [ ! -d "${STATS_DATA_PATH}" ] ; then
    echo "Invalid stats directory: ${STATS_DATA_PATH} does not exist or is not a directory"
    exit 2
fi

if [ -z "$*" ]; then
    STATS_SCRIPT_PARAMS="--upload_all_dates"
else
    STATS_SCRIPT_PARAMS="$*"
fi

CINSTALLER_PATH="/installer.sh"
CSTATS_DATA_PATH="/statsmart_data"
CSTATS_SCRIPT="/statsmart.sh"

# must be auth'ed with vault to retrieve xdp secrets for license and default admin
if ! vault token lookup >/dev/null 2>&1 ; then
    echo "Vault must be set up and you must be auth'ed to use this script"
    exit 2
fi


# create a temp script to run inside the container to kick things off
# sets install path to /var/opt/xcalar and assumes running as xcalar:xcalar
# default user is admin:admin
STATS_SCRIPT=$(mktemp -t cstatsmart_script.XXXXXX)
cat << EOF > "${STATS_SCRIPT}"
export XLRDIR="/opt/xcalar" #must unset XRLDIR in case it was set on the host
DEFAULT_ADMIN_FILE="/var/opt/xcalar/config/defaultAdmin.json"
${CINSTALLER_PATH} --start
echo '{ "username": "admin", "password": "6d51d4b15ded3bc357f6f1547de49cc81579e6a3b1ec85bbf50dcca20618d1c4", "defaultAdminEnabled": true, "email": "admin@localhost"}' > \${DEFAULT_ADMIN_FILE}
chown xcalar: \${DEFAULT_ADMIN_FILE}
chmod 600 \${DEFAULT_ADMIN_FILE}
echo "$(vault kv get -field=licenseDev secret/xdp)" | base64 -d | gunzip > /etc/xcalar/XcalarLic.key
export XLR_PYSDK_VERIFY_SSL_CERT=false
export XCE_HTTPS_PORT=443
\$XLRDIR/bin/python3 $XLRDIR/scripts/upload_stats.py -r ${CSTATS_DATA_PATH} ${STATS_SCRIPT_PARAMS}
EOF

trap "rm -f ${STATS_SCRIPT}" EXIT


#export DOCKER_VOL_MAPPING="${STATS_PATH}:/stats_to_load ${INSTALLER_PATH}:/installer.sh"
export DOCKER_VOL_MAPPING="${INSTALLER_PATH}:${CINSTALLER_PATH} ${STATS_SCRIPT}:${CSTATS_SCRIPT} ${STATS_DATA_PATH}:${CSTATS_DATA_PATH}"
export DOCKER_PORT_MAPPING="9443:443 10000:10000" # https and sqldf/jdbc
export CONTAINER_IMAGE="registry.int.xcalar.com/xcalar/el7-systemd:v5" # use basic systemd image to run the installer
export CONTAINER_UID=0 # need root in cshell to run installer, default xcalar 1000 does not have sudo

cshell bash -e ${CSTATS_SCRIPT}
