#!/usr/bin/env python

import json
import os
import argparse

configFileRoot = ""
hostsFile = "hosts.txt"
hosts = ()
privHostsFile = "privHosts.txt"
privHosts = ()
ldapConfigFile = "ldapConfig.json"
ldapConfig = {"ldapConfigEnabled": False}
hotPatchFile = "hotPatch.json"
hotPatch = False
licenseFile = "XcalarLic.key"
xcLicense = ""
xcalarMount = {}
xcalarRoot = ""
xcalarDemandPaging = ""
xcalarMountCreated = ""

parser = argparse.ArgumentParser()

parser.add_argument(
    "-c", "--configFileRoot", help="location of the config file directory")
parser.add_argument(
    "-s", "--mountServer", help="server portion of the xcalar root mount")
parser.add_argument(
    "-p", "--mountPath", help="path portion of the xcalar root mount")
parser.add_argument("-r", "--xcalarRoot", help="location of xcalar root")
parser.add_argument("-d", "--xcalarDemandPaging", help="DemandPaging path")
parser.add_argument("-m", "--xcalarMount", help="is mount Xcalar created")

args = parser.parse_args()

if args.configFileRoot:
    configFileRoot = args.configFileRoot

if args.mountPath:
    xcalarMount['path'] = args.mountPath

if args.mountServer:
    xcalarMount['server'] = args.mountServer

if args.xcalarRoot:
    xcalarRoot = args.xcalarRoot

if args.xcalarDemandPaging:
    xcalarDemandPaging = args.xcalarDemandPaging

if args.xcalarMount:
    xcalarMountCreated = args.xcalarMount

hostsPath = os.path.join(configFileRoot, hostsFile)

if os.path.isfile(hostsPath):
    with open(hostsPath, 'r') as f:
        hosts = [line.rstrip() for line in f]

privHostsPath = os.path.join(configFileRoot, privHostsFile)

if os.path.isfile(privHostsPath):
    with open(privHostsPath, 'r') as f:
        privHosts = [line.rstrip() for line in f]

ldapConfigPath = os.path.join(configFileRoot, ldapConfigFile)

if os.path.isfile(ldapConfigPath):
    with open(ldapConfigPath, 'r') as f:
        ldapConfig = json.load(f)

licensePath = os.path.join(configFileRoot, licenseFile)

if os.path.isfile(licensePath):
    with open(licensePath, 'r') as f:
        xcLicense = f.readline()

hotPatchPath = os.path.join(configFileRoot, hotPatchFile)

if os.path.isfile(hotPatchPath):
    with open(hotPatchPath, 'r') as f:
        hotPatchConfig = json.load(f)
        hotPatch = hotPatchConfig['hotPatchEnabled']

if (args.mountServer in hosts or \
    args.mountServer in privHosts) and \
    args.mountPath == '/srv/xcalar':
    xcalarMount['option'] = "xcalarNfs"
elif not args.mountServer or xcalarMountCreated == "1":
    xcalarMount['option'] = "readyNfs"
else:
    xcalarMount['option'] = "customerNfs"

print(
    json.dumps({
        "hosts": hosts,
        "privHosts": privHosts,
        "ldapConfig": ldapConfig,
        "license": xcLicense,
        "xcalarMount": xcalarMount,
        "xcalarRoot": xcalarRoot,
        "xcalarDemandPaging": xcalarDemandPaging,
        "enableHotPatches": hotPatch
    }))
