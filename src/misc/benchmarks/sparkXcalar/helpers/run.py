#!/bin/env python3.6
# Benchmark flow:
# Inputs:
#  - Existing resource group
#  - Retina file
# Process:
#  - ssh to single machine
#  - upload retina
#  - run retina
#  - get operation breakdown from retina run
# Outputs:
#  - Operation breakdown

import subprocess
import argparse
import json
import time
import logging
import random
import os

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s|%(levelname)s]: %(message)s')


def getResourceGroupHostname(resourceGroup):
    cmdArgs = [
        "az",
        "vm",
        "list-ip-addresses",
        "-g", resourceGroup
    ]
    logging.debug(" ".join(cmdArgs))
    pr = subprocess.Popen(cmdArgs, stdout=subprocess.PIPE)

    out = pr.communicate()[0]

    if pr.returncode != 0:
        raise ValueError("{} failed with return {}".format(
            " ".join(cmdArgs), pr.returncode))

    parsedOut = json.loads(out)
    if parsedOut == []:
        raise ValueError("No resource group named {}".format(resourceGroup))

    logging.info("parsedout ({}) elm 0  is {}".format(
        type(parsedOut),
        parsedOut[0]))
    ips = []
    for node in parsedOut:
        try:
            ips.append(
                node["virtualMachine"]["network"]
                    ["publicIpAddresses"][0]["ipAddress"])
        except IndexError:
            pass
    ips = [ip for ip in ips if ip]
    if len(ips) == 0:
        raise ValueError("No public IPs found for resource group!")
    elif len(ips) > 1:
        logging.info("WARNING: {} ips specified; choosing {}".format(
            len(ips),
            ips[0]))
    return ips[0]


class BenchRunner():
    def __init__(self, resourceGroup, user, password, dataflowPath, datasetUrl, pubIp=None):
        self.resourceGroup = resourceGroup
        self.user = user
        self.password = password
        if pubIp:
            self.hostname = pubIp
        else:
            self.hostname = getResourceGroupHostname(self.resourceGroup)
        self.remoteDir = "/tmp/bench-{}".format(random.randint(0, 2**40))
        self.remoteDataflow = os.path.join(self.remoteDir,
                                           os.path.basename(dataflowPath))
        self.remoteScript = os.path.join(self.remoteDir, "remote.py")
        self.dataflowPath = dataflowPath
        self.datasetUrl = datasetUrl

    def ssh(self, cmd, captureOut=True):
        sshCmd = [
            "sshpass", "-p", self.password,
            "ssh",
            "{user}@{host}".format(user=self.user, host=self.hostname),
        ] + cmd
        logging.debug(" ".join(sshCmd))
        outType = subprocess.PIPE if captureOut else None
        pr = subprocess.Popen(sshCmd,
                              stdout=outType, stdin=subprocess.PIPE)
        time.sleep(1)
        out = pr.communicate()[0]
        if pr.returncode != 0:
            raise ValueError("failed to execute '{}'".format(" ".join(sshCmd)))
        return out

    def scp(self, localPath, remotePath):
        sshCmd = [
            "sshpass", "-p", self.password,
            "scp",
            localPath,
            "{user}@{host}:{remotePath}".format(
                user=self.user,
                host=self.hostname,
                remotePath=remotePath),
        ]
        logging.debug(" ".join(sshCmd))
        pr = subprocess.Popen(sshCmd,
                              stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        time.sleep(1)
        out = pr.communicate()[0]
        if pr.returncode != 0:
            raise ValueError("failed to connect to host")
        return out

    def run(self):
        logging.info("Using directory '{}'".format(self.remoteDir))
        self.ssh(["mkdir", "-p", self.remoteDir])
        self.ssh(["whoami"])
        self.scp(self.dataflowPath, self.remoteDataflow)
        self.scp("remote.py", self.remoteScript)
        self.ssh(["/opt/xcalar/bin/python2.7",
                 self.remoteScript, "-d", self.remoteDataflow,
                 "-s", self.datasetUrl],
                 captureOut=False)


def main():
    parser = argparse.ArgumentParser(
            description="Run retina on azure resource group")
    parser.add_argument("-r", "--resourceGroup",
                        required=True,
                        help="azure resource group")
    parser.add_argument("-u", "--user",
                        required=True,
                        help="user with which to ssh to the cluster")
    parser.add_argument("-p", "--password",
                        required=True,
                        help="password with which to ssh to the cluster")
    parser.add_argument("-b", "--dataflow",
                        required=True,
                        help="path to batch dataflow")
    parser.add_argument("-s", "--dataseturl",
                        required=True,
                        help="dataset url in azblob")
    parser.add_argument("-i", "--pubip",
                        default=None,
                        help="public IP to run dataflow on")

    args = parser.parse_args()

    runner = BenchRunner(
            args.resourceGroup,
            args.user,
            args.password,
            args.dataflow,
            args.dataseturl,
            args.pubip)
    runner.run()


if __name__ == "__main__":
    main()
