# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import os
import sys
import tarfile
import base64
import io
import inspect


# inStr contains a base-64 encoded, gzipped tar archive. The files within the archive are
# unpacked to /opt/xcalar/scripts.
def main(inStr):
    raw = base64.b64decode(inStr)
    sio = io.StringIO(raw)

    with tarfile.open(fileobj=sio, mode='r:gz') as archive:
        archive.extractall("/opt/xcalar/scripts")


# Creates gzipped tar archive and prints base64 encoding to be passed as input to app.
if __name__ == "__main__":

    archivePath = sys.argv[1]
    archiveItems = sys.argv[2:]

    print(("Creating archive %s with items %s" % (archivePath, archiveItems)))

    with tarfile.open(archivePath, 'w:gz') as archive:
        for item in archiveItems:
            archive.add(item)

    with open(archivePath, 'r:b') as rawFile:
        encoded = base64.b64encode(rawFile.read())
        print(encoded)

    try:
        from xcalar.external.app import App
        from xcalar.external.client import Client
    except ImportError:
        print("Client not available")
        sys.exit(1)

    client = Client(bypass_proxy=True)

    apps = App(client)
    source = inspect.getsource(inspect.getmodule(main))
    outputSet = apps.set_py_app("Depends", source)
    if outputSet.output.hdr.status != 0:
        print("Failed to upload Depends App")
        sys.exit(1)

    outputRun = apps.run_py_app("Depends", True, encoded)
    if outputRun.output.hdr.status != 0:
        print("Failed to run Depends App")
        sys.exit(1)
