# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

# ldapEl7Installer.py - implements LdapInstallerEl7, which overrides a few
# functions from LdapInstaller to enable installation on CentOS 7

import logging
import subprocess
import os
import pwd
import grp
import shutil

from ldapElInstaller import LdapInstallerEl


class LdapInstallerEl7(LdapInstallerEl):
    def __init__(self, domain, password, fqdn, org, dbType, caPath):
        super(LdapInstallerEl7, self).__init__(domain, password, fqdn, org,
                                               dbType, caPath)
        self.accessModifyOpType = "add"

    def installPackages(self):
        installCmd = "yum -y install openldap-servers openldap-clients openldap"
        if (self.localInstall):
            installCmd = "yum -y localinstall {0}/openldap-*.rpm".format(
                self.rpmDir)
        reInstallCmd = "yum -y reinstall openldap"
        startCmd = "/usr/bin/systemctl start slapd.service"
        svcCmd = "/usr/bin/systemctl enable slapd.service"
        logging.debug("Running command: {0}".format(installCmd))
        if not self.dryRun:
            subprocess.check_call(installCmd, shell=True)
            subprocess.check_call(reInstallCmd, shell=True)
            subprocess.check_call(startCmd.split())
            subprocess.check_call(svcCmd.split())

    def installSchemaRoot(self, password, firstDomain, domainDN, org):

        logging.debug("Loading extra schemas")
        schemaFiles = [
            '/etc/openldap/schema/cosine.ldif',
            '/etc/openldap/schema/inetorgperson.ldif',
            '/etc/openldap/schema/nis.ldif'
        ]

        for file in schemaFiles:
            logging.debug(" - loading schema {0}".format(file))
            if not self.dryRun:
                self.runAsAdminExternal(file, domainDN)

        super(LdapInstallerEl7, self).installSchemaRoot(
            password, firstDomain, domainDN, org)

    def setupServices(self):
        slapdLog = os.path.join(self.slapdLogDirName, self.slapdLogName)
        firewallRunning = False
        rsyslogdContent = """
# slapd message logging
local4.* {0}
""".format(slapdLog)

        if not self.dryRun:
            ldapUid = pwd.getpwnam("ldap").pw_uid
            ldapGid = grp.getgrnam("ldap").gr_gid

            f = open(self.rsyslogdName, 'w+', 644)
            f.write(rsyslogdContent)
            f.close

            if (not os.path.exists(self.slapdLogDirName)):
                os.mkdir(self.slapdLogDirName, 0o755)

            os.chown(self.slapdLogDirName, ldapUid, ldapGid)

        iptablesCmd = "/usr/bin/firewall-cmd --state"
        logging.debug("Running command: {0}".format(iptablesCmd))
        if not self.dryRun:
            retCode = subprocess.call(iptablesCmd.split())
            if (retCode == 0):
                firewallRunning = True

        if (firewallRunning):
            iptablesCmd = "/usr/bin/firewall-cmd --permanent --add-service=ldap"
            logging.debug("Running command: {0}".format(iptablesCmd))
            if not self.dryRun:
                subprocess.check_call(iptablesCmd.split())

        slapdRestartCmd = "/usr/bin/systemctl restart slapd.service"
        logging.debug("Running command: {0}".format(slapdRestartCmd))
        if not self.dryRun:
            subprocess.check_call(slapdRestartCmd.split())

        rsyslogRestartCmd = "/usr/bin/systemctl condrestart rsyslog.service"
        logging.debug("Running command: {0}".format(rsyslogRestartCmd))
        if not self.dryRun:
            subprocess.call(rsyslogRestartCmd.split())

        if (firewallRunning):
            iptablesRestartCmd = "/usr/bin/firewall-cmd --reload"
            logging.debug("Running command: {0}".format(iptablesRestartCmd))
            if not self.dryRun:
                subprocess.call(iptablesRestartCmd.split())

    def eraseServiceConfig(self):
        firewallRunning = False
        isSyslogD = False

        if not self.dryRun:
            if (os.path.exists(self.rsyslogdName)):
                os.remove(self.rsyslogdName)
                isSyslogD = True

            if (os.path.exists(self.slapdLogDirName)):
                shutil.rmtree(self.slapdLogDirName)

        iptablesCmd = "/usr/bin/firewall-cmd --state"
        logging.debug("Running command: {0}".format(iptablesCmd))
        if not self.dryRun:
            retCode = subprocess.call(iptablesCmd.split())
            if (retCode == 0):
                firewallRunning = True

        if (firewallRunning):
            iptablesCmd = "/usr/bin/firewall-cmd --permanent --delete-service=ldap"
            detectCmd = "/usr/bin/firewall-cmd --query-service=ldap"
            iptablesRestartCmd = "/usr/bin/firewall-cmd --reload"
            logging.debug("Running command: {0}".format(iptablesCmd))
            if not self.dryRun:
                detectFlag = subprocess.call(detectCmd.split())
                logging.debug("detectFlag is {0}".format(detectFlag))
                if (detectFlag == 0):
                    subprocess.call(iptablesCmd.split())
                    logging.debug(
                        "Running command: {0}".format(iptablesRestartCmd))
                    subprocess.check_call(iptablesRestartCmd.split())

        rsyslogRestartCmd = "/usr/bin/systemctl condrestart rsyslog.service"
        logging.debug("Running command: {0}".format(rsyslogRestartCmd))
        if not self.dryRun and isSyslogD:
            subprocess.call(rsyslogRestartCmd.split())
