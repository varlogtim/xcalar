# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

# ldapEl6Installer.py - implements LdapInstallerEl6, which overrides a few
# functions from LdapInstaller to enable installation on CentOS 6

import logging
import subprocess
import os
import pwd
import grp
import shutil

from ldapElInstaller import LdapInstallerEl


class LdapInstallerEl6(LdapInstallerEl):
    def __init__(self, domain, password, fqdn, org, dbType, caPath):
        super(LdapInstallerEl6, self).__init__(domain, password, fqdn, org,
                                               dbType, caPath)
        self.renameOlcDB = False
        self.servicesSetup = False
        self.accessModifyOpType = "replace"

    def installPackages(self):
        installCmd = "yum -y install openldap openldap-servers openldap-clients"
        if (self.localInstall):
            installCmd = "yum -y localinstall {0}/openldap-*.rpm".format(
                self.rpmDir)
        startCmd = "/sbin/service slapd start"
        svcCmd = "/sbin/chkconfig slapd on --level 345"
        logging.debug("Running command: {0}".format(installCmd))
        if not self.dryRun:
            subprocess.check_call(installCmd, shell=True)
            subprocess.check_call(startCmd.split())
            subprocess.check_call(svcCmd.split())

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

        iptablesCmd = "/sbin/service iptables status"
        logging.debug("Running command: {0}".format(iptablesCmd))
        if not self.dryRun:
            retCode = subprocess.call(iptablesCmd.split())
            if (retCode == 0):
                firewallRunning = True

        if (firewallRunning):
            iptablesCmd = "/sbin/iptables -I INPUT -m tcp -p tcp --dport 389 -j ACCEPT"
            logging.debug("Running command: {0}".format(iptablesCmd))
            if not self.dryRun:
                subprocess.check_call(iptablesCmd.split())

        slapdRestartCmd = "/sbin/service slapd restart"
        logging.debug("Running command: {0}".format(slapdRestartCmd))
        if not self.dryRun:
            subprocess.check_call(slapdRestartCmd.split())

        rsyslogRestartCmd = "/sbin/service rsyslog condrestart"
        logging.debug("Running command: {0}".format(rsyslogRestartCmd))
        if not self.dryRun:
            subprocess.call(rsyslogRestartCmd.split())

        if (firewallRunning):
            iptablesSaveCmd = "/sbin/service iptables save"
            logging.debug("Running command: {0}".format(iptablesSaveCmd))
            if not self.dryRun:
                subprocess.call(iptablesSaveCmd.split())

    def eraseServiceConfig(self):
        global rsyslogdName
        global dryRun
        global slapdLogDirName
        firewallRunning = False
        isSyslogD = False

        if not self.dryRun:
            if (os.path.exists(self.rsyslogdName)):
                os.remove(self.rsyslogdName)
                isSyslogD = True

            if (os.path.exists(self.slapdLogDirName)):
                shutil.rmtree(self.slapdLogDirName)

        iptablesCmd = "/sbin/service iptables status"
        logging.debug("Running command: {0}".format(iptablesCmd))
        if not self.dryRun:
            retCode = subprocess.call(iptablesCmd.split())
            if (retCode == 0):
                firewallRunning = True

        if (firewallRunning):
            detectCmd = "/sbin/iptables -C INPUT -m tcp -p tcp --dport 389 -j ACCEPT"
            iptablesCmd = "/sbin/iptables -D INPUT -m tcp -p tcp --dport 389 -j ACCEPT"
            iptablesSaveCmd = "/sbin/service iptables save"
            logging.debug("Running command: {0}".format(iptablesCmd))
            if not self.dryRun:
                detectFlag = subprocess.call(detectCmd.split())
                if (detectFlag == 0):
                    subprocess.check_call(iptablesCmd.split())
                    logging.debug(
                        "Running command: {0}".format(iptablesSaveCmd))
                    subprocess.check_call(iptablesSaveCmd.split())

        rsyslogRestartCmd = "/sbin/service rsyslog condrestart"
        logging.debug("Running command: {0}".format(rsyslogRestartCmd))
        if not self.dryRun and isSyslogD:
            subprocess.call(rsyslogRestartCmd.split())
