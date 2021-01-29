# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

# ldapBaseInstaller.py - implements the LdapInstaller object that consists
# of basic methods to load LDIF from files, a wrapper that can call those
# functions and turn a provided LDIF string into a temporary file, and
# several generic install steps.

import logging
import subprocess
import tempfile

from ldapLdif import LdapLdif

rsyslogdName = '/etc/rsyslog.d/91-slapd.conf'
slapdLogDirName = '/var/log/slapd'
slapdLogName = 'slapd.log'
dryRun = False


class LdapInstaller(LdapLdif):
    def __init__(self, domain, password, fqdn, org, dbType, caPath):
        self.domainDN = "dc=" + domain.replace('.', ',dc=')
        self.domain = domain
        self.firstDomain = domain.split('.')[0]
        self.password = password
        self.tempPassword = "installerPassword"
        self.fqdn = fqdn
        self.org = org
        self.tlsDict = ""
        self.dbType = dbType
        self.caPath = caPath
        self.step = 0
        self.nsteps = 0

        self.installPackage = False
        self.encryptedPassword = False
        self.schemaRootInstalled = False
        self.memberOfOverlay = False
        self.refintOverlay = False
        self.schemaInstalled = False
        self.accessLevelsSet = False

        self.rsyslogdName = rsyslogdName
        self.slapdLogDirName = slapdLogDirName
        self.slapdLogName = slapdLogName
        self.dryRun = dryRun

    def runAsAdminExternal(self, fileName, dn):
        cmd = "ldapadd -Y EXTERNAL -H ldapi:/// -D cn=admin,{0} -f {1}".format(
            dn, fileName)
        logging.debug("Running command: {0}".format(cmd))
        if not self.dryRun:
            subprocess.check_call(cmd.split())

    def runAsAdmin(self, fileName, password, dn):
        cmd = "ldapadd -x -w {0} -H ldapi:/// -D cn=admin,{1} -f {2}".format(
            password, dn, fileName)
        logging.debug("Running command: {0}".format(cmd))
        if not self.dryRun:
            subprocess.check_call(cmd.split())

    def runAsRoot(self, fileName):
        cmd = "ldapmodify -Q -Y EXTERNAL -H ldapi:/// -f {0}".format(fileName)
        logging.debug("Running command: {0}".format(cmd))
        if not self.dryRun:
            subprocess.check_call(cmd.split())

    def encryptPassword(self, password):
        cmd = "/usr/sbin/slappasswd -h {{SSHA}} -n -s {0}".format(password)
        logging.debug("Running command: {0}".format(cmd))
        if not self.dryRun:
            proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
            return proc.communicate()[0].decode("utf-8")
        else:
            return ""

    def installTempFile(self, data, func, *args):
        temp = tempfile.NamedTemporaryFile(mode='w+t')
        try:
            temp.write(data)
            temp.flush()

            func(temp.name, *args)
        except subprocess.CalledProcessError:
            raise
        finally:
            temp.close()

    def installSchemaRoot(self, password, firstDomain, domainDN, org):
        schemaRoot = self.schemaRootLdif(domainDN, firstDomain, org)

        logging.debug("Loading LDIF file:\n {0}".format(schemaRoot))
        self.installTempFile(schemaRoot, self.runAsAdmin, password, domainDN)

    def installMemberOfOverlay(self, path, dbNum, domainDN, dbType):
        memberOfOverlayLdif = self.memberOfOverlayLdif(path, dbNum, dbType)

        logging.debug("Loading LDIF file:\n {0}".format(memberOfOverlayLdif))
        self.installTempFile(memberOfOverlayLdif, self.runAsAdminExternal,
                             domainDN)

    def installRefintOverlay(self, path, dbNum, domainDN, dbType):
        refintOverlayLdif = self.refintOverlayLdif(path, dbNum, dbType)

        logging.debug("Loading LDIF file:\n {0}".format(refintOverlayLdif))
        self.installTempFile(refintOverlayLdif, self.runAsAdminExternal,
                             domainDN)

    def installSchema(self, password, domainDN, domain, encryptedPassword):
        schema = self.schemaLdif(domainDN)

        logging.debug("Loading LDIF file:\n {0}".format(schema))
        self.installTempFile(schema, self.runAsAdmin, password, domainDN)

        adminUser = self.adminUserLdif(domainDN, domain, encryptedPassword)
        logging.debug("Loading LDIF file:\n {0}".format(adminUser))
        self.installTempFile(adminUser, self.runAsAdmin, password, domainDN)

        adminUserGroup = self.adminUserGroupLdif(domainDN, domain)
        logging.debug("Loading LDIF file:\n {0}".format(adminUserGroup))
        self.installTempFile(adminUserGroup, self.runAsAdmin, password,
                             domainDN)

    def setAccessLevels(self, domainDN, dbType, opType):
        ldapAccess = self.accessLevelLdif(domainDN, dbType, opType)

        logging.debug("Loading LDIF file:\n {0}".format(ldapAccess))
        self.installTempFile(ldapAccess, self.runAsAdminExternal, domainDN)

    def setTLSDict(self, tlsDict):
        self.tlsDict = tlsDict

    def installPackages(self):
        raise NotImplementedError

    def logStep(self, msg):
        self.step += 1
        logging.info("Step [{0}/{1}]: {2}".format(self.step, self.nsteps, msg))

    def logStepInit(self, count):
        self.step = 0
        self.nsteps = count

    def do(self):
        raise NotImplementedError
