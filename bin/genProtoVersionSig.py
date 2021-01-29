#!/usr/bin/env python

# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import argparse
import hashlib
import glob
import os
import sys

def parseArgs():
    parser = argparse.ArgumentParser(description='Version signature generator')
    parser.add_argument('-d', '--dir', help='Directory to proto files that need to be versioned', required=True)
    parser.add_argument('-o', '--output', help='Output of signature file. Default to stdout')
    return parser.parse_args()

def main():
    args = parseArgs()
    file_list = sorted(glob.glob(args.dir + "*.proto"))
    print(file_list)
    total = ""
    #default to stdoutput if output was not specified
    handle = open(args.output, 'w') if args.output else sys.stdout
    handle.write("enum XcRpcApiVersion {\n")
    for file in file_list:
        file_name = os.path.splitext(os.path.basename(file))[0]
        print(file_name)
        versionSig = hashlib.md5(open(file, "rb").read()).hexdigest()
        total += versionSig + '\n'
        handle.write("\t%sVersionSignature:\"%s\" = %d,\n" % (file_name, versionSig, int(versionSig[0:7], 16)))
    versionSig = hashlib.md5(total.encode("utf-8")).hexdigest()
    handle.write("\tProtoAPIVersionSignature:\"%s\" = %d,\n" % (versionSig, int(versionSig[0:7], 16)))
    handle.write("}\n")
    if handle is not sys.stdout:
        handle.close()

if __name__ == "__main__":
    main()
