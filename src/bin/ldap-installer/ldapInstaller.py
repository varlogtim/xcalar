# Copyright 2016 Xcalar, Inc. All rights reserved.s
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

# ldapInstaller.py - top level script that parses command line arguments,
# creates objects, and kicks off the install

import optparse
import logging
import json
import os
import platform

from ldapEl6Installer import LdapInstallerEl6
from ldapEl7Installer import LdapInstallerEl7
from sslCaInstaller import OpenSSLCA

sslCaDirName = '/opt/ca'
# this is paired with ldap_config_file in cluster-install.sh
xceConfigDir = '/tmp'
xceConfigFile = 'ldapConfig.json'


def write_config_file(fqdn, domain, srvCertPath):
    scriptPath = os.path.dirname(os.path.realpath(__file__))
    jsonPath = os.path.abspath(xceConfigDir)
    jsonFilePath = os.path.join(jsonPath, xceConfigFile)
    domainDN = "dc=" + domain.replace('.', ',dc=')

    jsonObj = {
        'ldap_uri':
            "ldap://{0}:389".format(fqdn),
        'userDN':
            "mail=%username%,ou=People,{0}".format(domainDN),
        'useTLS':
            True,
        'searchFilter':
            "(memberof=cn=xceUsers,ou=Groups,{0})".format(domainDN),
        'activeDir':
            False,
        'serverKeyFile':
            srvCertPath,
        'ldapConfigEnabled':
            True
    }
    try:
        os.makedirs(jsonPath)
    except OSError:
        if not os.path.isdir(jsonPath):
            logging.error(
                "[FAILURE] error writing out config file because directory {0} does not exist and could not be created"
                .format(jsonPath))
            exit(-1)

    f = open(jsonFilePath, 'w+', 644)
    json.dump(jsonObj, f, indent=4)
    f.close()


def detect_system():
    if (platform.system() != 'Linux'):
        return "none"

    (distro, version) = platform.linux_distribution()[0:2]
    version = version.split('.')[0]
    version = int(float(version))

    if (not (distro.startswith('CentOS')
             or distro.startswith('Red Hat Enterprise Linux Server'))):
        return "none"

    if (version == 6):
        return "el6"
    elif (version == 7):
        return "el7"
    else:
        return "none"


def main(options):
    execObj = ""
    sslObj = ""
    system = "none"

    if (options.system == "none"):
        system = detect_system()
    else:
        system = options.system

    if (system == "none"):
        logging.error('[FAILURE] system unknown')
        exit(-1)

    if (options.debug):
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    logging.info('Started')

    if (options.tls):
        sslObj = OpenSSLCA(sslCaDirName, options.domain, options.password,
                           options.addr, options.org)

    if (system == "el6"):
        if (options.command == 'erase'):
            execObj = LdapInstallerEl6("", "", "", "", "", "")
        else:
            execObj = LdapInstallerEl6(options.domain, options.password,
                                       options.addr, options.org, "bdb",
                                       sslCaDirName)

    if (system == "el7"):
        if (options.command == 'erase'):
            execObj = LdapInstallerEl7("", "", "", "", "", "")
        else:
            execObj = LdapInstallerEl7(options.domain, options.password,
                                       options.addr, options.org, "hdb",
                                       sslCaDirName)

    if (options.command == 'erase'):
        logging.info("Starting {0} OpenLDAP erase".format(options.system))
        execObj.doErase()
        logging.info("Starting the OpenSSL CA erase")
        sslObj.doErase()
    else:
        if (options.tls):
            logging.info("Starting the OpenSSL CA installation")
            try:
                logging.info("Step [1/3]: Setup SSL...")
                sslObj.setup()
                logging.info("...Done.\n")
                logging.info("Step [2/3]: Create signing request...")
                sslObj.createReq('ldap')
                logging.info("...Done.\n")
                logging.info("Step [3/3]: Sign request...")
                sslObj.sign('ldap')
                logging.info("...Done.\n")
            except Exception as e:
                logging.error("[FAILURE] {0}".format(e))
                exit(-1)

            execObj.setTLSDict({
                'caCertPath': sslObj.getCaCertPath(),
                'svrKeyPath': sslObj.getSvrKeyPath(),
                'svrCertPath': sslObj.getSvrCertPath()
            })

        logging.info("Starting {0} OpenLDAP installation".format(
            options.system))
        execObj.do()

    if (options.dump):
        logging.info('Writing out config file...')
        try:
            write_config_file(options.addr, options.domain,
                              sslObj.getSvrCertPath())
        except Exception as e:
            logging.error("[FAILURE] {0}".format(e))
            exit(-1)

    logging.info('[SUCCESS] Finished')


if __name__ == "__main__":
    parser = optparse.OptionParser(
        usage="usage: %prog -c install | erase [options]", version="%prog 0.9")

    parser.add_option(
        "-c",
        "--command",
        dest='command',
        type="choice",
        choices=['install', 'erase'],
        help="choose to install or erase OpenLDAP [and TLS certificates]")
    parser.add_option(
        "-s",
        "--system",
        dest='system',
        default='none',
        type="choice",
        choices=['el6', 'el7', 'none'],
        help="operating system platform for deployment")

    parser.add_option(
        "-d",
        "--domain",
        dest='domain',
        help="domain where the LDAP is running, example: foo.com")
    parser.add_option(
        "-p",
        "--password",
        dest='password',
        help="root administrator password for the LDAP service, "
        "example: welcome123")
    parser.add_option(
        "-a",
        "--addr",
        dest='addr',
        help="full hostname with domain or ip address of the host "
        "running LDAP, example: bar.foo.com, 192.168.1.100")
    parser.add_option(
        "-o",
        "--org",
        dest='org',
        help="name of the organization or part of the organization "
        "running the LDAP, example: Foo, Inc.")
    parser.add_option(
        "-t",
        "--withouttls",
        action='store_false',
        dest='tls',
        default=True,
        help="do not create TLS certificate authority and "
        "install certificates during OpenLDAP install")
    parser.add_option(
        "-g",
        "--debug",
        action='store_true',
        dest='debug',
        default=False,
        help="turn on debugging messages")
    parser.add_option(
        "-u",
        "--dump",
        action='store_true',
        dest='dump',
        default=False,
        help="dump Xi configuration to JSON file")

    (options, args) = parser.parse_args()

    top_mandatories = ['command', 'system']
    for m in top_mandatories:
        if (not options.__dict__[m]):
            print("mandatory option {0} is missing\n".format(m))
            parser.print_help()
            exit(-1)

    if (options.command == 'install'):
        cmd_mandatories = ['domain', 'password', 'addr', 'org']
        for m in cmd_mandatories:
            if (not options.__dict__[m]):
                print("mandatory install option {0} is missing\n".format(m))
                parser.print_help()
                exit(-1)

    main(options)
