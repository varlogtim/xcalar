# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

# ldapInstaller.py - top level script that parses command line arguments,
# creates objects, and kicks off the install

import subprocess
import os
import shutil
import random
import socket
import logging


class OpenSSLCA:
    def __init__(self, path, domain, password, addr, org):
        self.caPath = path
        self.domain = domain
        self.password = password
        self.addr = addr
        self.org = org

        self.caCertPath = ""
        self.svrKeyPath = ""
        self.svrCertPath = ""

    def setup(self):
        opensslCnf = """
HOME                    = .
RANDFILE                = $ENV::HOME/.rnd

[ ca ]
default_ca      = CA_default

[ CA_default ]
dir             = .
new_certs_dir   = $dir/newcerts
crl_dir         = $dir/crl
database        = $dir/index

certificate     = $dir/ca-cert.pem
serial          = $dir/serial
crl             = $dir/ca-crl.pem
private_key     = $dir/private/ca-key.pem
RANDFILE        = $dir/private/.rand

x509_extensions = usr_cert
copy_extensions = copy

# Allow two certificates to have the same subject
unique_subject  = no

# Comment out the following two lines for the "traditional"
# format.
name_opt        = ca_default
cert_opt        = ca_default

default_crl_days= 30
default_days    = 730
default_md      = sha1
preserve        = no

policy          = policy_match

# For the CA policy
[ policy_match ]
countryName             = match
stateOrProvinceName     = match
organizationName        = match
organizationalUnitName  = optional
commonName              = supplied
emailAddress            = optional

# For the 'anything' policy
[ policy_anything ]
countryName             = optional
stateOrProvinceName     = optional
localityName            = optional
organizationName        = optional
organizationalUnitName  = optional
commonName              = supplied
emailAddress            = optional

####################################################################
[ req ]
default_bits            = 2048
default_keyfile         = ./private/ca-key.pem
default_md              = sha1

prompt                  = no
distinguished_name      = root_ca_distinguished_name

x509_extensions = v3_ca

# If passwords for private keys are not present, the user running the
# installer will be prompted for them
# input_password = secret
# output_password = secret

# This sets a mask for permitted string types. There are several options.
# default: PrintableString, T61String, BMPString.
# pkix   : PrintableString, BMPString.
# utf8only: only UTF8Strings.
# nombstr : PrintableString, T61String (no BMPStrings or UTF8Strings).
# MASK:XXXX a literal mask value.
string_mask = nombstr

req_extensions = v3_req

[ root_ca_distinguished_name ]
commonName = {0} {4}
countryName = US
stateOrProvinceName = California
localityName = San Jose
0.organizationName = {1}
emailAddress = ca_owner@{1}

[ usr_cert ]

# These extensions are added when 'ca' signs a request.
basicConstraints=CA:FALSE

# PKIX recommendations harmless if included in all certificates.
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer:always

nsCaRevocationUrl               = https://{2}/ca-crl.pem
#nsBaseUrl
#nsRevocationUrl
#nsRenewalUrl
#nsCaPolicyUrl
#nsSslServerName

[ v3_req ]

# Extensions to add to a certificate request
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = @alt_names

[alt_names]
{3}

[ v3_sign ]

subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always,issuer:always

basicConstraints = CA:FALSE
keyUsage = nonRepudiation,digitalSignature

[ v3_ca ]

# Extensions for a typical CA

# PKIX recommendation.
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always,issuer:always

basicConstraints = CA:true

[ crl_ext ]

# CRL extensions.
# Only issuerAltName and authorityKeyIdentifier
# make any sense in a CRL.

# issuerAltName=issuer:copy
authorityKeyIdentifier=keyid:always,issuer:always
""".format(self.org, self.domain, self.addr, self.createAltNames(),
           random.randrange(0, 1000000))

        os.mkdir(self.caPath, 0o755)

        myCwd = os.getcwd()
        os.chdir(self.caPath)

        f = open('openssl.cnf', 'w+', 664)
        f.write(opensslCnf)
        f.close

        os.mkdir('crl', 0o775)
        os.mkdir('newcerts', 0o775)
        os.mkdir('private', 0o750)

        f = open('serial', 'w+', 664)
        f.write('01')
        f.close()

        f = open('index', 'w+', 664)
        f.close()

        cmd = 'openssl req -nodes -config openssl.cnf -days 1825 -x509 -newkey rsa:4096 -out ca-cert.pem -outform PEM'
        subprocess.check_call(cmd.split())

        self.caCertPath = self.caPath + '/ca-cert.pem'
        os.chdir(myCwd)

    def createAltNames(self):
        ipList = []
        nameList = []
        result = ""
        nameCnt = 1
        ipCnt = 1

        logging.debug("checking format of {0}".format(self.addr))

        try:
            socket.inet_aton(self.addr)
            logging.debug("format of {0} is ip".format(self.addr))
            ipList.append(str(self.addr))
        except socket.error:
            logging.debug("format of {0} is name".format(self.addr))
            nameList.append(str(self.addr))

        logging.debug("adding localhost addresses")
        nameList.append("localhost")
        ipList.append("127.0.0.1")

        for name in nameList:
            logging.debug("adding name {0}".format(name))
            result += "DNS.{0} = {1}\n".format(nameCnt, name)
            nameCnt += 1

        for ip in ipList:
            logging.debug("adding ip {0}".format(ip))
            result += "IP.{0} = {1}\n".format(ipCnt, ip)
            ipCnt += 1

        return result

    def createReq(self, reqRoot):
        myCwd = os.getcwd()
        os.chdir(self.caPath)

        # Even though this was originally designed to create an encrypted key
        # with a password, it turns out that OpenLDAP can't use such a key.
        # See section 16.2.1.4 here:
        # http://www.openldap.org/doc/admin24/tls.html
        cmd = 'openssl req -new -newkey rsa:2048 -days 730 -out {0}.csr -keyout private/{0}-key.pem -nodes -extensions v3_req -config ./openssl.cnf'.format(
            reqRoot, self.password)
        subprocess.check_call(cmd.split())

        self.svrKeyPath = self.caPath + '/private/{0}-key.pem'.format(reqRoot)

        os.chdir(myCwd)

    def sign(self, reqRoot):
        myCwd = os.getcwd()
        os.chdir(self.caPath)

        cmd = 'openssl ca -batch -config ./openssl.cnf -in {0}.csr -out {0}.cert'.format(
            reqRoot)
        subprocess.check_call(cmd.split())

        self.svrCertPath = self.caPath + '/{0}.cert'.format(reqRoot)

        os.chdir(myCwd)

    def revoke(self, reqRoot):
        myCwd = os.getcwd()
        os.chdir(self.caPath)

        cmd = 'openssl ca -config ./openssl.cnf -revoke {0}.cert'.format(
            reqRoot)
        subprocess.check_call(cmd.split())

        os.chdir(myCwd)

        self.gencrl()

    def gencrl(self):
        myCwd = os.getcwd()
        os.chdir(self.caPath)

        cmd = 'openssl ca -config ./openssl.cnf -gencrl -out ca-crl.pem'
        subprocess.check_call(cmd.split())

        os.chdir(myCwd)

    def getCaCertPath(self):
        return self.caCertPath

    def getSvrKeyPath(self):
        return self.svrKeyPath

    def getSvrCertPath(self):
        return self.svrCertPath

    def doErase(self):
        if (os.path.exists(self.caPath)):
            shutil.rmtree(self.caPath)
