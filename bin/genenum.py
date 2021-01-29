#!/usr/bin/env python3.6

# Copyright 2014 - 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

# given a .enum file generate a .c, .h or a .thrift file.  this allows
# developers to edit a single source (the .enum file) rather than manually
# keep both a .thrift and a .h file in sync.  see src/bin/mgmt/MgmtDaemon.cpp
# for why this is needed.  alternatively this can be used to define an enum
# along with helper routines to convert it to & from a string

import sys
import re
import argparse
import os
import collections

primeNumbers = [
    2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71,
    73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151,
    157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233,
    239, 241, 251, 257
]


class Enum:
    def __init__(self):
        self.name = ""
        self.valDict = dict()
        self.strDict = dict()
        self.intValDict = dict()


def parseArgs():
    parser = argparse.ArgumentParser(description='Enumeration Code Generator')
    parser.add_argument('-i', '--input', help='Input file', required=True)
    parser.add_argument('-e', '--eprefix',
                        help='Generate a C source file with an error prefix')
    outputGroup = parser.add_mutually_exclusive_group(required=True)
    outputGroup.add_argument('-c', '--csrc', help='Generate a C source file')
    outputGroup.add_argument('-r', '--chdr', help='Generate a C header file')
    outputGroup.add_argument('-t', '--thrift', help='Generate a thrift file')
    outputGroup.add_argument('-j', '--json', help='Generate a json file')
    outputGroup.add_argument('-p', '--proto', help='Generate a proto file')
    outputGroup.add_argument('-y', '--python', help='Generate a python file')
    return parser.parse_args()


def camelToSnake(name):
    a = re.compile('((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))')
    return a.sub(r'_\1', name).upper()


def parseEnums(args):
    fileContent = ""
    inFile = open(args.input, "r")
    for line in inFile:
        line = line.strip()    # trim whitespace
        line = line.split('#')[0]    # trim comments
        if len(line) == 0:    # trim empty lines
            continue
        fileContent += line
    inFile.close()

    enumList = list()

    # There are 4 valid input forms:
    # 1.
    # enum <EnumName> {
    #     <EnumValue1>, <- valPatNoStr
    #     <EnumValue2>,
    #     ...
    # }
    # 2.
    # enum <EnumName>
    #     <EnumValue1> = <val1>, <- valPatNoStrInts
    #     <EnumValue2> = <val2>,
    #     ...
    # }
    # 3.
    # enum <EnumName> {
    #     <EnumValue1>:<EnumValue1String>, <- valPatWithStr
    #     <EnumValue2>:<EnumValue2String>,
    #     ...
    # }
    # 4.
    # enum <EnumName> {
    #     <EnumValue1>:<EnumValue1String> = <val1>, <- valPatWithStrInts
    #     <EnumValue2>:<EnumValue2String> = <val2>,
    #     ...
    # }
    declPat = re.compile(r'enum[\s]*([\w]*)[\s]*\{(.*?)}')

    valPatWithStrInts = re.compile(
        r'[\s]*([\w]*)[\s]*:[\s]*(\".*?\")[\s]*=[\s]*([A-Fa-f0-9x]*),')
    valPatNoStrInts = re.compile(
        r'[\s]*([\w]*)[\s]*=[\s]*([A-Fa-f0-9x]*)[\s]*,')
    valPatWithStr = re.compile(r'[\s]*([\w]*)[\s]*:[\s]*(\".*?\")[\s]*,')
    valPatNoStr = re.compile(r'[\s]*([\w]*)[\s]*,')

    for declPatMatch in re.finditer(declPat, fileContent):
        enumName = re.sub(declPat, r'\1', declPatMatch.group(0))
        enumVals = re.sub(declPat, r'\2', declPatMatch.group(0))
        found = False
        enum = Enum()
        enum.name = enumName
        count = 0
        for valPatWithStrIntsMatch in re.finditer(valPatWithStrInts, enumVals):
            enumVal = re.sub(valPatWithStrInts, r'\1',
                             valPatWithStrIntsMatch.group(0))
            enumStr = re.sub(valPatWithStrInts, r'\2',
                             valPatWithStrIntsMatch.group(0))
            enumIntVal = re.sub(valPatWithStrInts, r'\3',
                                valPatWithStrIntsMatch.group(0))
            enum.valDict[count] = enumVal
            enum.strDict[count] = enumStr
            enum.intValDict[count] = enumIntVal
            found = True
            count = count + 1
        if found is True:
            enumList.append(enum)
            continue

        for valPatWithStrMatch in re.finditer(valPatWithStr, enumVals):
            enumVal = re.sub(valPatWithStr, r'\1', valPatWithStrMatch.group(0))
            enumStr = re.sub(valPatWithStr, r'\2', valPatWithStrMatch.group(0))
            enum.valDict[count] = enumVal
            enum.strDict[count] = enumStr
            enum.intValDict[count] = None
            found = True
            count = count + 1
        if found is True:
            enumList.append(enum)
            continue

        for valPatNoStrIntsMatch in re.finditer(valPatNoStrInts, enumVals):
            enumVal = re.sub(valPatNoStrInts, r'\1',
                             valPatNoStrIntsMatch.group(0))
            enumStr = "\"" + enumVal + "\""
            enumIntVal = re.sub(valPatNoStrInts, r'\2',
                                valPatNoStrIntsMatch.group(0))
            enum.valDict[count] = enumVal
            enum.strDict[count] = enumStr
            enum.intValDict[count] = enumIntVal
            found = True
            count = count + 1
        if found is True:
            enumList.append(enum)
            continue

        for valPatNoStrMatch in re.finditer(valPatNoStr, enumVals):
            enumVal = re.sub(valPatNoStr, r'\1', valPatNoStrMatch.group(0))
            enumStr = "\"" + enumVal + "\""
            enum.valDict[count] = enumVal
            enum.strDict[count] = enumStr
            enum.intValDict[count] = None
            count = count + 1
        enumList.append(enum)

    return enumList


def writeCopyright(inFileName, outFile, prefix="//"):
    outFile.write(
        "{}\n{} **********************************************************************\n{} *** DO NOT EDIT!  This file was autogenerated by genenum.py        ***\n{} *** Please edit\n{} ".format(prefix, prefix, prefix, prefix, prefix)
    )
    outFile.write(inFileName)
    outFile.write(
        "\n{}     instead\n{} **********************************************************************\n{}\n{} Copyright 2014-2019 Xcalar, Inc. All rights reserved.\n{}\n{} No use, or distribution, of this source code is permitted in any form or\n{} means without a valid, written license agreement with Xcalar, Inc.\n{} Please refer to the included \"COPYING\" file for terms and conditions\n{} regarding the use and redistribution of this software.\n{}\n{} **********************************************************************\n{} *** DO NOT EDIT!  This file was autogenerated by genenum.py        ***\n{} *** Please edit\n{} ".format(prefix, prefix, prefix, prefix, prefix, prefix, prefix, prefix, prefix, prefix, prefix, prefix, prefix, prefix)
    )
    outFile.write(inFileName)
    outFile.write(
        "\n{}     instead\n{} **********************************************************************\n{}\n\n".format(prefix, prefix, prefix)
    )
    return


def calcTableSize(enum):
    tableSize = len(enum.valDict)
    for val in enum.valDict:
        if enum.intValDict[val] is not None and int(
                enum.intValDict[val]) >= tableSize:
            tableSize = int(enum.intValDict[val]) + 1
    return tableSize


def getNearestPrime(number):
    for prime in primeNumbers:
        if prime > number:
            return (prime)


def genSparseMap(enum, tableSize, outFile, prefix=None):
    primeNumber = getNearestPrime(len(enum.valDict))
    nodes = dict()
    nodeSize = dict()

    outStr = "\nstatic const int PrimeNumber = %d;\n\n" % primeNumber
    outFile.write(outStr)

    outStr = "typedef struct Node {\n"
    outStr += "    size_t key;\n"
    outStr += "    const char *value;\n"
    outStr += "} Node;\n\n"
    outFile.write(outStr)

    outStr = "typedef struct Map {\n"
    outStr += "    size_t numElems;\n"
    outStr += "    Node *nodeArray;\n"
    outStr += "} Map;\n\n"
    outFile.write(outStr)

    for val in enum.valDict:
        key = int(enum.intValDict[val]) % primeNumber
        prefixStr = "" if (prefix is None) else "{}-{:08X} ".format(prefix, val)
        if key in nodes:
            nodes[key] += ",\n    { .key = " + enum.valDict[val]
            nodes[key] += ", .value = \"" + prefixStr + \
                enum.strDict[val].strip("\"") + "\" }"
            nodeSize[key] += 1
        else:
            nodes[key] = "    { .key = " + enum.valDict[val]
            nodes[key] += ", .value = \"" + prefixStr + \
                enum.strDict[val].strip("\"") + "\" }"
            nodeSize[key] = 1

    for key in nodes:
        outStr = "static Node node%d[%d] = {\n" % (key, nodeSize[key])
        outStr += nodes[key]
        outStr += "\n};\n\n"
        outFile.write(outStr)

    outStr = "static Map nodeMappings[%d] = {\n" % primeNumber
    for ii in range(0, primeNumber):
        if ii in nodes:
            key = ii
            outStr += "    nodeMappings[%d] = { .numElems = %d, .nodeArray = node%d },\n" % (
                key, nodeSize[key], key)
        else:
            outStr += "    nodeMappings[%d] = { .numElems = 0, .nodeArray = NULL },\n" % ii
    outStr += "};\n\n"
    outFile.write(outStr)

    outStr = "const char *\nstrGetFrom" + enum.name + "(" + enum.name + " x)\n{\n"
    outStr += "    size_t key = x % PrimeNumber;\n"
    outStr += "    size_t numElems = nodeMappings[key].numElems;\n"
    outStr += "    Node *nodeArray = nodeMappings[key].nodeArray;\n"
    outStr += "    unsigned ii;\n"
    outStr += "    for (ii = 0; ii < numElems; ii++) {\n"
    outStr += "        if (nodeArray[ii].key == x) {\n"
    outStr += "            return (nodeArray[ii].value);\n"
    outStr += "        }\n"
    outStr += "    }\n"
    outStr += "    return (NULL);\n"
    outStr += "}\n\n"
    outFile.write(outStr)

    # XXX Implement strToEnumName
    outStr = enum.name + "\nstrTo" + enum.name + "(const char *str)\n{\n"
    outStr += "    // XXX Implement me\n"
    outStr += "    assert(\"Function not implemented!\" && false);\n"
    outStr += "    return ((" + enum.name + ") " + str(tableSize) + ");\n"
    outStr += "}\n\n"
    outFile.write(outStr)

    # XXX Implement isValidEnumName
    outStr = "bool\nisValid" + enum.name + "(" + enum.name + " x)\n{\n"
    outStr += "    // XXX Implement me\n"
    outStr += "    assert(\"Function not implemented!\" && false);\n"
    outStr += "    return (false);\n"
    outStr += "}"
    outFile.write(outStr)


def genCompactMap(enum, tableSize, outFile, prefix=None):
    outStr = "\n#define Str" + enum.name + "TableSize (" + str(
        tableSize) + ")\n\n"
    outFile.write(outStr)
    outStr = "static const char\n *str" + enum.name + "Table[Str" + enum.name + "TableSize" + "] = {\n"
    outFile.write(outStr)
    for val in enum.valDict:
        prefixStr = "" if (prefix is None) else "{}-{:08X} ".format(prefix, val)
        outStr = "    [" + enum.valDict[val] + "] = \"" + prefixStr + enum.strDict[
            val].strip("\"") + "\",\n"
        outFile.write(outStr)
    outStr = "};\n\n"
    outFile.write(outStr)

    # strIsValidEnumName
    outStr = "bool\nisValid" + enum.name + "(" + enum.name + " x)\n{\n"
    outStr += "    if ((unsigned)x >= Str" + enum.name + "TableSize) {\n"
    outStr += "        return (false);\n"
    outStr += "    }\n"
    outStr += "    return (str" + enum.name + "Table[x] != NULL);\n"
    outStr += "}\n\n"
    outFile.write(outStr)

    # strToEnumName
    outStr = enum.name + "\nstrTo" + enum.name + "(const char *str)\n{\n"
    outStr += "    unsigned ii;\n"
    outStr += "    for (ii = 0; ii < Str" + enum.name + "TableSize; ii++) {\n"
    outStr += "        if (strcmp(str, str" + enum.name + "Table[ii]) == 0) {\n"
    outStr += "            return (" + enum.name + ") (ii);\n"
    outStr += "        }\n"
    outStr += "    }\n"
    outStr += "    return (" + enum.name + ") (ii);\n"
    outStr += "}\n\n"
    outFile.write(outStr)

    # strGetFromEnumName
    outStr = "const char *\nstrGetFrom" + enum.name + "(" + enum.name + " x)\n{\n"
    outStr += "    if (!isValid" + enum.name + "(x)) {\n"
    outStr += "        assert(\"Unknown EnumName!\" && false);\n"
    outStr += "        return (NULL);\n"
    outStr += "    }\n"
    outStr += "    return (str" + enum.name + "Table[x]);\n"
    outStr += "}"
    outFile.write(outStr)


def genCSource(args, enumList):
    outFile = open(args.csrc, "w")
    writeCopyright(args.input, outFile)
    outFile.write(
        "#include <assert.h>\n#include <string.h>\n#include <stdbool.h>\n#include <cstdlib>\n\n"
    )
    (filePrefix, fileExtension) = args.csrc.split('.')
    if filePrefix == "Status" or filePrefix == "Subsys":
        filePrefix = "primitives/" + filePrefix
    elif filePrefix == "LibApisEnums" or filePrefix == "XcalarApiVersionSignature":
        filePrefix = "libapis/" + filePrefix
    outStr = "#include \"" + filePrefix + ".h\"\n\n"
    outFile.write(outStr)
    for enum in enumList:
        tableSize = calcTableSize(enum)
        if (tableSize > (len(enum.valDict) * 10)):
            genSparseMap(enum, tableSize, outFile)
        else:
            genCompactMap(enum, tableSize, outFile,
                          args.eprefix if args.eprefix else None)

    outFile.close()
    return


def genCHeader(args, enumList):
    outFile = open(args.chdr, "w")
    writeCopyright(args.input, outFile)
    (filePrefix, fileExtension) = args.chdr.split('.')
    headerTag = "_" + filePrefix.upper() + "_H_"
    outStr = "#ifndef " + headerTag + "\n#define " + headerTag + "\n\n"
    outFile.write(outStr)
    for enum in enumList:
        lastVal = ""
        outFile.write("typedef enum {\n")
        for val in enum.valDict:
            if enum.intValDict[val] is None:
                outStr = "    " + enum.valDict[val] + ",\n"
            else:
                outStr = "    " + enum.valDict[val] + " = " + enum.intValDict[
                    val] + ",\n"
            lastVal = enum.valDict[val]
            outFile.write(outStr)
        outStr = "} " + enum.name + ";\n"
        outFile.write(outStr)
        outStr = "#define " + enum.name + "Len (" + lastVal + " + 1)\n\n"
        outFile.write(outStr)
        outStr = "const char *strGetFrom" + enum.name + "(" + enum.name + " x);\n"
        outFile.write(outStr)
        outStr = enum.name + " strTo" + enum.name + "(const char * str);\n\n"
        outFile.write(outStr)
        outStr = "bool isValid" + enum.name + "(" + enum.name + " x);\n\n"
        outFile.write(outStr)

    outStr = "#endif // " + headerTag + "\n"
    outFile.write(outStr)
    outFile.close()
    return


def genThrift(args, enumList):
    outFile = open(args.thrift, "w")
    writeCopyright(args.input, outFile)

    # XXX 6/21/18 we want slightly different names for the C side and thrift
    # side
    for enum in enumList:
        if enum.name == "StatusCode":
            enum.name = "Status"
            for k, v in enum.valDict.items():
                assert v.startswith("StatusCode")
                newValue = v.replace("StatusCode", "Status")
                enum.valDict[k] = newValue

    for enum in enumList:
        outStr = "enum " + enum.name + "T {\n"
        outFile.write(outStr)
        for val in enum.valDict:
            if enum.intValDict[val] is None:
                outStr = "    " + enum.valDict[val] + " = " + str(val) + ",\n"
            else:
                outStr = "    " + enum.valDict[val] + " = " + enum.intValDict[
                    val] + ",\n"
            outFile.write(outStr)
        outFile.write("}\n\n")
        outStr = "const map<" + enum.name + "T,string> " + enum.name + "TStr = {\n"
        outFile.write(outStr)
        for val in enum.valDict:
            outStr = "    " + enum.name + "T." + enum.valDict[
                val] + ":" + enum.strDict[val] + ",\n"
            outFile.write(outStr)
        outFile.write("}\n\n")
        outStr = "const map<string," + enum.name + "T> " + enum.name + "TFromStr = {\n"
        outFile.write(outStr)
        for val in enum.valDict:
            outStr = "    " + enum.strDict[
                val] + ":" + enum.name + "T." + enum.valDict[val] + ",\n"
            outFile.write(outStr)
        outFile.write("}\n\n")

    outFile.close()
    return


def genJson(args, enumList):
    # keep consistent for naming convention
    for enum in enumList:
        if enum.name == "StatusCode":
            enum.name = "Status"
            for k, v in enum.valDict.items():
                assert v.startswith("StatusCode")
                newValue = v.replace("StatusCode", "Status")
                enum.valDict[k] = newValue

    for enum in enumList:
        try:
            path = args.json + "/" + enum.name
            os.mkdir(path)
        except FileExistsError:
            pass
        enumToIntFile = open(path + "/" + enum.name + "ToInt.json", "w")
        enumToStrFile = open(path + "/" + enum.name + "ToStr.json", "w")
        strToEnumFile = open(path + "/" + enum.name + "FromStr.json", "w")
        enumToIntFile.write("{\n")
        enumToStrFile.write("{\n")
        strToEnumFile.write("{\n")
        keys = list(enum.valDict.keys())
        for key in keys:
            c = ","
            if key == keys[-1]:
                c = ""
            if enum.intValDict[key] is None:
                enumToIntStr = "    " + "\"" + enum.valDict[key] + "\"" + " : " + str(key) + c + "\n"
                enumToStrStr = "    " + "\"" + str(key) + "\"" + " : " + enum.strDict[key] + c + "\n"
                strToEnumStr = "    " + enum.strDict[key] + " : " + str(key) + c + "\n"
            else:
                enumToIntStr = "    " + "\"" + enum.valDict[key] + "\"" + " : " + enum.intValDict[
                    key] + c + "\n"
                enumToStrStr = "    " + "\"" + enum.intValDict[key] + "\"" + " : " + enum.strDict[
                    key] + c + "\n"
                strToEnumStr = "    " + enum.strDict[key] + " : " + enum.intValDict[
                    key] + c + "\n"
            enumToIntFile.write(enumToIntStr)
            enumToStrFile.write(enumToStrStr)
            strToEnumFile.write(strToEnumStr)
        enumToIntFile.write("}\n\n")
        enumToStrFile.write("}\n\n")
        strToEnumFile.write("}\n\n")
        enumToIntFile.close()
        enumToStrFile.close()
        strToEnumFile.close()

def genProto(args, enumList):
    # keep consistent for naming convention
    for enum in enumList:
        if enum.name == "StatusCode":
            enum.name = "Status"
            for k, v in enum.valDict.items():
                assert v.startswith("StatusCode")
                newValue = v.replace("StatusCode", "Status")
                enum.valDict[k] = newValue
    outFile = open(args.proto, "w")
    writeCopyright(args.input, outFile)
    outStr = """

syntax = "proto3";

package xcalar.compute.localtypes.XcalarEnumType;

"""
    outFile.write(outStr)
    for enum in enumList:
        outStr = "enum " + enum.name + " {\n"
        outFile.write(outStr)
        unique = True
        value_occurrences = collections.Counter(enum.valDict.values())
        for key in value_occurrences:
            if value_occurrences[key] > 1:
                unique = False
        if not unique:
            outStr = "    option allow_alias = true;\n"
            outFile.write(outStr)
        for val in enum.valDict:
            if enum.intValDict[val] is None:
                outStr = "    " + camelToSnake(enum.valDict[val]) + " = " + str(val) + ";\n"
            else:
                outStr = "    " + camelToSnake(enum.valDict[val]) + " = " + enum.intValDict[
                    val] + ";\n"
            outFile.write(outStr)
        outFile.write("}\n\n")
    outFile.close()

def genPython(args, enumList):
    # keep consistent for naming convention
    for enum in enumList:
        if enum.name == "StatusCode":
            enum.name = "Status"
            for k, v in enum.valDict.items():
                assert v.startswith("StatusCode")
                newValue = v.replace("StatusCode", "Status")
                enum.valDict[k] = newValue
    outFile = open(args.python, "w")
    writeCopyright(args.input, outFile, "#")
    outStr = "import enum\n\n"
    outFile.write(outStr)
    for enum in enumList:
        if not enum.intValDict:
            outStr = "class " + enum.name + " (enum.Enum):\n"
        else:
            outStr = "class " + enum.name + " (enum.IntEnum):\n"
        outFile.write(outStr)
        for val in enum.valDict:
            if enum.intValDict[val] is None:
                outStr = "    " + enum.valDict[val] + ",\n"
            else:
                outStr = "    " + enum.valDict[val] + " = " + enum.intValDict[
                    val] + ",\n"
            outFile.write(outStr)
        outFile.write("\n")
    outFile.close()

def main():
    args = parseArgs()
    enumList = parseEnums(args)

    if (args.csrc):
        genCSource(args, enumList)
    elif (args.chdr):
        genCHeader(args, enumList)
    elif (args.json):
        genJson(args, enumList)
    elif (args.proto):
        genProto(args, enumList)
    elif (args.python):
        genPython(args, enumList)
    else:
        genThrift(args, enumList)

    sys.exit(0)


if __name__ == "__main__":
    main()
