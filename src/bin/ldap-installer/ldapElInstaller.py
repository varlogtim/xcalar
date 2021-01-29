# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

# ldapElInstaller.py - implements LdapInstallerEl, the base installer for
# OpenLDAP installation on CentOS/RH platforms with stub methods for those
# parts of the install that are version-specific

import logging
import subprocess
import os
import grp
import glob

from ldapBaseInstaller import LdapInstaller


class LdapInstallerEl(LdapInstaller):
    def __init__(self, domain, password, fqdn, org, dbType, caPath):
        super(LdapInstallerEl, self).__init__(domain, password, fqdn, org,
                                              dbType, caPath)
        self.renameOlcDB = False
        self.servicesSetup = False
        self.accessModifyOpType = "replace"
        self.rpmDir = "/var/tmp/xcalar-extra"

        localServerRpms = glob.glob(self.rpmDir + '/openldap-servers*.rpm')
        localClientRpms = glob.glob(self.rpmDir + '/openldap-clients*.rpm')
        self.localInstall = (len(localServerRpms) == 1
                             and len(localClientRpms) == 1)

    def installPackages(self):
        pass

    def erasePackages(self):
        cmd = "yum -y erase openldap-servers openldap-clients"
        rmCmd = "rm -rf /etc/openldap/schema /etc/openldap/slapd* /etc/openldap/check_password.conf /var/lib/ldap"
        logging.debug("Running command: {0}".format(cmd))
        if not self.dryRun:
            subprocess.check_call(cmd.split())
            subprocess.check_call(rmCmd, shell=True)

    def renameOlcDatabase(self, domainDN, password, dbType):
        ldifRenameDB = self.renameOlcDbLdif(domainDN, password, dbType)

        logging.debug("Loading LDIF file:\n {0}".format(ldifRenameDB))
        self.installTempFile(ldifRenameDB, self.runAsRoot)

    def changeOlcRootPW(self, password, dbType):
        ldifRootPW = self.changeOlcRootPWLdif(password, dbType)

        logging.debug("Loading LDIF file:\n {0}".format(ldifRootPW))
        self.installTempFile(ldifRootPW, self.runAsRoot)

    def setupServices(self):
        raise NotImplementedError

    def eraseServiceConfig(self):
        raise NotImplementedError

    def configureSSL(self):
        tlsLdif = self.sslConfigLdif(self.tlsDict['caCertPath'],
                                     self.tlsDict['svrCertPath'],
                                     self.tlsDict['svrKeyPath'])

        logging.debug(
            "Changing the group owner of the TLS private key directory {0}".
            format(self.caPath + '/private'))
        ldapGid = grp.getgrnam("ldap").gr_gid
        os.chown(self.caPath + '/private', -1, ldapGid)

        logging.debug("Loading LDIF file:\n {0}".format(tlsLdif))
        self.installTempFile(tlsLdif, self.runAsRoot)

    def do(self):
        try:
            #
            # logStep provides a count/total log of progress
            # example: Step [3/10]: Renaming olcDatabase{2}...
            nsteps = 11 if (self.tlsDict != "") else 10
            self.logStepInit(nsteps)

            # Step 1
            self.logStep("Installing OpenLDAP packages...")
            self.installPackages()
            self.installPackage = True
            logging.info("...Done.\n")

            # Step 2
            self.logStep("Encrypting temp administrator password...")
            self.encryptedPassword = self.encryptPassword(self.tempPassword)
            self.encryptPassword = True
            logging.info("...Done.\n")

            # Step 3
            self.logStep("Renaming olcDatabase{2}...")
            self.renameOlcDatabase(self.domainDN, self.encryptedPassword,
                                   self.dbType)
            self.renameOlcDB = True
            logging.info("...Done.\n")

            # Step 4
            self.logStep("Setting operating system services...")
            self.setupServices()
            self.servicesSetup = True
            logging.info("...Done.\n")

            # Step 5
            self.logStep("Installing schema root...")
            self.installSchemaRoot(self.tempPassword, self.firstDomain,
                                   self.domainDN, self.org)
            self.schemaRootInstalled = True
            logging.info("...Done.\n")

            # Step 6
            self.logStep("Installing memberOf overlay...")
            self.installMemberOfOverlay("/usr/lib64/openldap", 2,
                                        self.domainDN, self.dbType)
            self.memberOfOverlay = True
            logging.info("...Done.\n")

            # Step 7
            self.logStep("Installing refInt overlay...")
            self.installRefintOverlay("/usr/lib64/openldap", 2, self.domainDN,
                                      self.dbType)
            self.refintOverlay = True
            logging.info("...Done.\n")

            # Step 8
            self.logStep("Installing schema...")
            self.installSchema(self.tempPassword, self.domainDN, self.domain,
                               self.password)
            self.schemaInstalled = True
            logging.info("...Done.\n")

            # Step 9
            self.logStep("Setting access levels...")
            self.setAccessLevels(self.domainDN, self.dbType,
                                 self.accessModifyOpType)
            self.accessLevelsSet = True
            logging.info("...Done.\n")

            # Step 10
            self.logStep(
                "Changing password to provided administrator password...")
            self.changeOlcRootPW(self.password, self.dbType)
            logging.info("...Done.\n")

            # Step 11
            if (self.tlsDict != ""):
                self.logStep("Starting TLS configuration...")
                self.configureSSL()
                logging.info("...Done.\n")

        except subprocess.CalledProcessError as e:
            logging.error("[FAILURE] {0}".format(e))
            exit(-1)

    def doErase(self):
        try:
            self.erasePackages()
            self.eraseServiceConfig()

        except subprocess.CalledProcessError:
            pass
