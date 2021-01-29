# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import os
import shutil

def doesThisFileExist(pathToFile):
    try:
        if ((os.path.exists(pathToFile) is True) and (os.path.isfile(pathToFile) is True)):
            return True
    except:
        if pathToFile is not None:
            print("File does not exist %s" % pathToFile)

    return False

def doesThisDirExist(pathToDir):
    try:
        if ((os.path.exists(pathToDir) is True) and (os.path.isdir(pathToDir))):
            return True
    except:
        print("Not a valid directory %s" % pathToDir)

    return False

def listAllFiles(path):
    listOfFiles = []
    try:
        for root, directories, filenames in os.walk(path):
            for filename in filenames:
                listOfFiles.append(os.path.join(root,filename))
    except:
        print("Unable to list files on %s" % path)

    return listOfFiles

def createDirectoryIf(pathToDir):
    if (doesThisDirExist(pathToDir) is True):
        return (0)

    try:
        os.mkdir(pathToDir)
        return (0)
    except:
        print("Unable to create directory: " + pathToDir)
        return (1)

def copyFile(src, dst):
    try:
        shutil.copyfile(src, dst)
        return (0)
    except:
        print("Unable to copy file. src: " + src + " dst: " + dst)
        return (1)

def getContents(fileName):
    try:
        fileHandle = open(fileName, "r")
        contents = fileHandle.readlines()
        fileHandle.close()

        return contents

    except:
        print("Unable to open file" + fileName)
        return None

    finally:
        try:
            fileHandle.close()
        except:
            pass

def openFile(filePath, openMode):
    try:
        fileHandle = open(filePath, openMode, encoding = 'utf-8')
        return (fileHandle)

    except:
        print("Failed to open %s with %s" % (filePath, openMode))
        return (None)


def closeFile(fileHandle):
    try:
        fileHandle.close()
    except:
        print("Failed to close file")
