#!/bin/bash

XCALAR_EXTRA="/var/tmp/xcalar-extra"
LDAP_CLIENT_GLOB=$(ls ${XCALAR_EXTRA}/openldap-clients*)

if [ -d "$XCALAR_EXTRA" ] &&
    [ -n "$LDAP_CLIENT_GLOB" ]; then

    yum -y localinstall ${LDAP_CLIENT_GLOB}
else
    yum -y install openldap-clients
fi
exit $rc
