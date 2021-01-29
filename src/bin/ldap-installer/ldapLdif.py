# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

# ldapLdif.py - implements the LdapLdif object, which consists of templated
# chunks of LDIF that are used to configure OpenLDAP


class LdapLdif(object):
    def __init__(self):
        pass

    def schemaRootLdif(self, domainDN, firstDomain, org):
        return """
dn: {0}
objectClass: domain
dc: {1}
description: {2}
""".format(domainDN, firstDomain, org)

    def memberOfOverlayLdif(self, path, dbNum, dbType):
        return """
dn: cn=module,cn=config
cn: module
objectclass: olcModuleList
objectclass: top
olcmoduleload: memberof.la
olcmodulepath: {0}

dn: olcOverlay={{0}}memberof,olcDatabase={{{1}}}{2},cn=config
objectClass: olcConfig
objectClass: olcMemberOf
objectClass: olcOverlayConfig
objectClass: top
olcOverlay: memberof
olcMemberOfGroupOC: groupOfUniqueNames
olcMemberOfMemberAD: uniqueMember
""".format(path, dbNum, dbType)

    def refintOverlayLdif(self, path, dbNum, dbType):
        return """
dn: cn=module,cn=config
cn: module
objectclass: olcModuleList
objectclass: top
olcmoduleload: refint.la
olcmodulepath: {0}

dn: olcOverlay={{1}}refint,olcDatabase={{{1}}}{2},cn=config
objectClass: olcConfig
objectClass: olcOverlayConfig
objectClass: olcRefintConfig
objectClass: top
olcOverlay: {{1}}refint
olcRefintAttribute: memberof member manager owner
""".format(path, dbNum, dbType)

    def schemaLdif(self, domainDN):
        return """
dn: ou=People,{0}
ou: People
objectClass: organizationalUnit
objectClass: top

dn: ou=Groups,{0}
ou: Groups
objectClass: organizationalUnit
objectClass: top

dn: cn=administrators,ou=Groups,{0}
cn: administrators
objectClass: groupOfUniqueNames
objectClass: top
uniqueMember:

dn: cn=xceUsers,ou=Groups,{0}
cn: xceUsers
objectClass: groupOfUniqueNames
objectClass: top
uniqueMember:
""".format(domainDN)

    def adminUserLdif(self, domainDN, domain, passwordSSHA):
        return """
dn: mail=admin@{1},ou=People,{0}
mail: admin@{1}
objectClass: inetOrgPerson
objectClass: organizationalPerson
objectClass: person
objectClass: top
cn: Admin
sn: User
employeeType: administrator
userPassword: {2}
""".format(domainDN, domain, passwordSSHA)

    def adminUserGroupLdif(self, domainDN, domain):
        return """
dn: cn=administrators,ou=Groups,{0}
changetype: modify
add: uniqueMember
uniqueMember: mail=admin@{1},ou=People,{0}

dn: cn=administrators,ou=Groups,{0}
changetype: modify
delete: uniqueMember
uniqueMember:

dn: cn=xceUsers,ou=Groups,{0}
changetype: modify
add: uniqueMember
uniqueMember: mail=admin@{1},ou=People,{0}

dn: cn=xceUsers,ou=Groups,{0}
changetype: modify
delete: uniqueMember
uniqueMember:
""".format(domainDN, domain)

    def accessLevelLdif(self, domainDN, dbType, opType):
        return """
dn: olcDatabase={{2}}{1},cn=config
changetype: modify
{2}: olcAccess
olcAccess: {{0}}to attrs=userPassword,shadowLastChange
  by dn="cn=admin,{0}" write
  by group/groupOfUniqueNames/uniqueMember="cn=administrators,ou=Groups,{0}" write
  by anonymous auth
  by self write
  by * none
olcAccess: {{1}}to dn.base="" by * read
olcAccess: {{2}}to *
  by self write
  by dn="cn=admin,{0}" write
  by group/groupOfUniqueNames/uniqueMember="cn=administrators,ou=Groups,{0}" write
  by * read
olcAccess: {{3}}to dn.subtree="ou=People,{0}"
  by dn="cn=admin,{0}" write
  by group/groupOfUniqueNames/uniqueMember="cn=administrators,ou=Groups,{0}" write
  by anonymous auth
  by self write
  by * none
""".format(domainDN, dbType, opType)

    def renameOlcDbLdif(self, domainDN, password, dbType):
        return """
dn: olcDatabase={{2}}{2},cn=config
changetype: modify
replace: olcRootDN
olcRootDN: cn=admin,{0}

dn: olcDatabase={{2}}{2},cn=config
changetype: modify
replace: olcSuffix
olcSuffix: {0}

dn: olcDatabase={{1}}monitor,cn=config
changetype: modify
replace: olcAccess
olcAccess: {{0}}to *
  by dn.base="gidNumber=0+uidNumber=0,cn=peercred,cn=external,cn=auth" read
  by dn.base="cn=admin,{0}" read
  by * none

dn: olcDatabase={{0}}config,cn=config
changetype: modify
add: olcRootPW
olcRootPW: {1}

dn: olcDatabase={{2}}{2},cn=config
changetype: modify
add: olcRootPW
olcRootPW: {1}
""".format(domainDN, password, dbType)

    def changeOlcRootPWLdif(self, password, dbType):
        return """
dn: olcDatabase={{0}}config,cn=config
changetype: modify
replace: olcRootPW
olcRootPW: {0}

dn: olcDatabase={{2}}{1},cn=config
changetype: modify
replace: olcRootPW
olcRootPW: {0}
""".format(password, dbType)

    def sslConfigLdif(self, caCertPath, svrCertPath, svrKeyPath):
        return """
dn: cn=config
changetype: modify
delete: olcTLSCACertificatePath
-
add: olcTLSCACertificateFile
olcTLSCACertificateFile: {0}
-
replace: olcTLSCertificateFile
olcTLSCertificateFile: {1}
-
replace: olcTLSCertificateKeyFile
olcTLSCertificateKeyFile: {2}
""".format(caCertPath, svrCertPath, svrKeyPath)
