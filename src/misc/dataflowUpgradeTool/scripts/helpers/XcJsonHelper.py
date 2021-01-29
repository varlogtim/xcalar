# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import json

from helpers import fileHelper

class XcJsonHelper(object):

    def __init__(self, verbose = False):
        self.verbose = verbose

    def load(self, jsonFilePath):

        fileHandle = fileHelper.openFile(jsonFilePath, "r")
        if (fileHandle is None):
            return (None)

        try:
            jsonContents = json.load(fileHandle)
            fileHelper.closeFile(fileHandle)

            return (jsonContents)

        except:
            print("Unable to get json contents from: %s" % jsonFilePath)

        finally:
            fileHelper.closeFile(fileHandle)

    def getValue(self, key, jsonObject):
        try:

            keys = jsonObject.keys()
            if (key not in keys):
                print("Invalid key. Available: %s" % ("".join(keys)))
                return (None)

            return (jsonObject[key])
        except:
            print("Unable to get value for key: %s" % key)
            return (None)

    def dump(self, openFileHandle, jsonObject, indent = 4):
        try:
            json.dump(jsonObject, openFileHandle, indent = indent)
            return (0)
        except:
            return (1)
