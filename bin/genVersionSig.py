#!/usr/bin/env python

# Copyright 2014 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import sys
import argparse
import hashlib

def parseArgs():
    parser = argparse.ArgumentParser(description='Version signature generator')
    parser.add_argument('-i', '--input', help='Space separated fully qualified file paths', required=True)
    parser.add_argument('-o', '--output', help='Output of signature file. Default to stdout')
    return parser.parse_args()

def main():
    args = parseArgs()
 
    hash_md5 = hashlib.md5()
    file_list = args.input.split()
    for file in file_list:
        hash_md5.update(open(file, 'rb').read())
    versionSig = hash_md5.hexdigest()

    #default to stdoutput if output was not specified
    handle = open(args.output, 'w') if args.output else sys.stdout
    handle.write("enum XcalarApiVersion {\n")
    handle.write("\tXcalarApiVersionSignature:\"%s\" = %d,\n" % (versionSig, int(versionSig[0:7], 16)))
    handle.write("}")
    if handle is not sys.stdout:
        handle.close()

    print("Input files for Version signature: \"{}\", Version signature: {}".format(', '.join(file_list), versionSig))

if __name__ == "__main__":
    main()
