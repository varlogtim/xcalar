#!/usr/bin/python2.7

# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

# This program takes in a Xcalar log file and extracts all protocol buffers to
# binary files for inspection by protobuf utilities.
#
# TODO: Implement ability to patch modified protobuf into existing log file
# reserved space

import struct
import math
import sys
import os
import getpass
import argparse
import base64
import types
from google.protobuf import descriptor

import google.protobuf.json_format as pbJson

sys.path.append(os.getenv('XLRDIR') + '/buildOut/src/lib/libprotobuf/')
sys.path.append(os.getenv('XLRDIR') + '/src/lib/libprotobuf/')
import DurableObject_pb2

parser = argparse.ArgumentParser()
parser.add_argument('-f', dest='fin', required=True,
                    help='Xcalar log file from which to extract protobufs')
parser.add_argument('-F', dest='fout', required=True,
                    help='Extraction output directory')
parser.add_argument('-p', dest='protobin', required=False, action='store_true',
                    help='Dump to raw protobuf binary files instead of JSON')

args = parser.parse_args()

def xcalarFieldToJsonObject(self, field, value):
    if field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_MESSAGE:
        return self._MessageToJsonObject(value)
    elif field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_ENUM:
        enum_value = field.enum_type.values_by_number.get(value, None)
        if enum_value is not None:
            return enum_value.name
        else:
            # If the enum is invalid, print the value number in the field
            return '"UNKNOWN ENUM VALUE: ' + str(value) + '"'
    elif field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_STRING:
        if field.type == descriptor.FieldDescriptor.TYPE_BYTES:
            # Due to Xc-7730 we store strings as BytesValue for fixed size character arrays.
            # This logic attempts to decode them as such.  If attempt fails, treat it as
            # actual binary data (e.g. exportRetinaBuf) and base 64 encode it
            try:
                value.decode('utf-8')
                return value
            except UnicodeDecodeError:
                return base64.b64encode(value).decode('utf-8')
        else:
            return value
    elif field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_BOOL:
        return bool(value)
    elif field.cpp_type in pbJson._INT64_TYPES:
        return str(value)
    elif field.cpp_type in pbJson._FLOAT_TYPES:
        if math.isinf(value):
            if value < 0.0:
                return _NEG_INFINITY
            else:
                return _INFINITY
        if math.isnan(value):
            return _NAN
    return value

# To work around Xc-7730 we need to override Google's _FieldToJsonObject method with
# our own...
xcalarMessageToJsonPrinter = pbJson._Printer(False)
# Bind our method to the object instance above
boundOverride_FieldToJsonObject = types.MethodType(xcalarFieldToJsonObject, xcalarMessageToJsonPrinter, pbJson._Printer)
# Actually override it
xcalarMessageToJsonPrinter._FieldToJsonObject = boundOverride_FieldToJsonObject
xcalarMessageToJson = xcalarMessageToJsonPrinter.ToJsonString

class dumpPb:
    def __init__(self):
        self.magic = 0xdead3c5deb96c17e

    def roundUp(self, val, base):
        return int(base * math.ceil(float(val)/base))

    def genFoutName(self, directory, prefix, count, isJson=False):
        prefix = os.path.basename(prefix)
        if isJson:
            suffix = '.json'
        else:
            suffix = '.pb.bin'
        fullPath = directory + '/' + prefix + '-' + format(count, '09d') + suffix
        return os.path.abspath(fullPath)

    def readQword(self, f):
        rawData = f.read(8)
        return struct.unpack('Q', rawData)[0]


    def loadAndDump(self):
        DurObj = DurableObject_pb2.DurableObject()
        print 'Loading '+args.fin
        with open(args.fin, 'rb') as fin:
            rawData = fin.read(8)
            pbCount = 0
            while rawData:
                currWord = struct.unpack('Q', rawData)[0]
                # Brute force search for magic...
                if currWord == self.magic:
                    # 8 byte checksum follows magic
                    cSum = self.readQword(fin)
                    # 8 byte header version
                    ver = self.readQword(fin)
                    assert(ver == 1)
                    # 8 byte allocated size of protobuf data
                    pbAllocSize = self.readQword(fin)
                    # 8 byte used size of protobuf data follows allocated
                    pbSize = self.readQword(fin)
                    # Then read in the raw pb data
                    pbData = fin.read(pbSize)

                    if args.protobin:
                        foutName = self.genFoutName(args.fout, args.fin, pbCount)
                        with open(foutName, 'wb') as fout:
                            fout.write(pbData)
                        print 'PROTOBUF RECORD %d: %d binary bytes (%d bytes allocated, checksum 0x%016x): %s'\
                              % (pbCount, pbSize, pbAllocSize, cSum, foutName)
                    else:
                        DurObj.ParseFromString(pbData)
                        foutName = self.genFoutName(args.fout, args.fin, pbCount, True)
                        with open(foutName, 'wb') as fout:
                            fout.write(xcalarMessageToJson(DurObj))
                        print 'JSON RECORD %d: %d binary bytes (%d bytes allocated, checksum 0x%016x): %s'\
                              % (pbCount, pbSize, pbAllocSize, cSum, foutName)

                    # PB data ends are not aligned
                    newPos = self.roundUp(fin.tell(), 8)
                    fin.seek(newPos)
                    pbCount += 1
                rawData = fin.read(8)

            return pbCount

    def printIns(self):
        if args.protobin:
            print
            print '*** Requires: apt-get install protobuf-compiler libprotobuf-dev'
            print 'To view and edit the protocol buffers:'
            print '0) Get the Java viewer/editor at:'
            print '     netstore:/netstore/users/ecohen/pbeditor/' +\
                  'ProtocolBuffer_Editor_0.98_Installer_pb_3.0.2.jar'
            print '1) Install with:'
            print '     java -jar ProtocolBuffer_Editor_0.98_Installer_pb_3.0.2.jar'
            print '2) Run: runEditor.sh'
            print '3) Paste (ctrl-v) in the "File" GUI field with an absolute path to a binary ' +\
                  'protobuf file dumped from this script.  For example:'
            print '     %s' % (os.path.abspath(self.genFoutName(args.fout, args.fin, 0)))
            print '4) Paste in the "Proto Definition" GUI field with an absolute ' +\
                  'path to the top-level IDL.  For example:'
            print '     %s/src/include/pb/durable/DurableObject.proto' % (os.getenv('XLRDIR'),)
            print '5) Leave the remaining GUI fields default and click the "Edit" button'
            print 'You should now see a graphical view of the protobuf data.'
            print 'Note in some cases the GUI retains state after an error and must be restarted'
            print 'Note that the column headings adjust depending on which element is selected'



myDumper = dumpPb()

count = myDumper.loadAndDump()
if count:
    print 'Extracted ' + str(count) + ' protobuf records from ' + args.fin
    myDumper.printIns()
else:
    print 'No protobuf records found in ' + args.fin
