# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
"""
ParquetToolParser used to parse the results of runnig parquet-tools.jar
"""

import json
import subprocess
import os
import base64
import tempfile
import shutil
from pyparsing import ParseException as ppParseException
from pyparsing import (Word, alphas, alphanums, Regex, Suppress, Forward,
                       Group, oneOf, ZeroOrMore, Optional, Keyword)
from .base import BaseParquetParser, ParseException


class ParquetToolsParser(BaseParquetParser):
    def __init__(self, parserName, logger):
        super(ParquetToolsParser, self).__init__(parserName, logger)
        self.schemaParser = None

    def makeSchemaParser(self):
        # Derived from protobuf_parser.py by Paul McGuire
        # Original at git://pkgs.fedoraproject.org/pyparsing/pyparsing-1.5.6/examples/protobuf_parser.py

        identifier = Word(alphas + "_", alphanums + "_").setName("identifier")
        schemaIdentifier = Word(
            alphas + "_", alphanums + "_" + ".").setName("schemaIdentifier")
        integer = Regex(r"[+-]?\d+")

        lbrace, rbrace, lparens, rparens, semicolon, comma = map(
            Suppress, "{}();,")

        keywords = "message required optional repeated true false option"

        keywordsDict = {}
        for keyword in keywords.split(" "):
            keywordsDict[keyword] = Keyword(keyword)

        # Define a message
        messageBody = Forward()
        messageDefinition = keywordsDict["message"] - schemaIdentifier(
            "schemaType") + lbrace + messageBody("body") + rbrace

        # Define a field in a message
        typespec = oneOf(
            "double float int32 int64 int96 uint32 uint64 bool string bytes binary group boolean"
        ) | Group("fixed_len_byte_array" + lparens + integer +
                  rparens) | identifier
        groupQualifier = oneOf("LIST MAP MAP_KEY_VALUE")
        binaryQualifier = oneOf("UTF8")
        decimalQualifier = Group("DECIMAL" + lparens + integer + comma +
                                 integer + rparens)
        integerQualifier = oneOf("INT_8 INT_16")
        typeQualifier = groupQualifier | binaryQualifier | decimalQualifier | integerQualifier
        fieldDefinitionHdr = (
            (keywordsDict["required"] | keywordsDict["optional"]
             | keywordsDict["repeated"])("fieldQualifier") -
            typespec("typespec") + identifier("identifier") +
            Optional(lparens + typeQualifier("typeQualifier") + rparens))

        fieldDefinition = Forward()
        fieldDefinition << Group(fieldDefinitionHdr + (semicolon | (
            lbrace + ZeroOrMore(fieldDefinition)("children") + rbrace)))

        messageBody << Group(ZeroOrMore(fieldDefinition)("fields"))

        return ZeroOrMore(messageDefinition)

    def parseParquetSchema(self, stdout):
        if self.schemaParser is None:
            self.schemaParser = self.makeSchemaParser()
        schemaParser = self.schemaParser
        fullBody = "\n".join([line.decode("UTF-8") for line in stdout])
        return schemaParser.parseString(fullBody, parseAll=True).asDict()

    def startswith(self, string, prefix):
        prefixTokens = self.split(prefix)
        for token in self.split(string):
            try:
                prefixToken = prefixTokens.__next__()
            except StopIteration as e:
                return True

            if token != prefixToken:
                return False

        try:
            prefixToken = prefixTokens.__next__()
            return False
        except StopIteration as e:
            return True

    def insertValue(self,
                    record,
                    fieldName,
                    xpath,
                    fieldValue,
                    appendMode=False,
                    columns=None):
        tmpRecord = record

        if columns is not None:
            fullPath = ".".join([path[0] for path in xpath] + [fieldName])
            found = False
            for column in columns:
                if isinstance(fieldValue, dict):
                    if self.startswith(fullPath, column) or self.startswith(
                            column, fullPath):
                        found = True
                        break
                elif self.startswith(fullPath, column):
                    found = True
                    break
            if not found:
                return

        if appendMode:
            searchPath = xpath[0:-1]
        else:
            searchPath = xpath

        for (field, fieldType) in searchPath:
            if fieldType is None:
                continue
            if fieldType["xcalarType"] == "DfObject":
                tmpRecord = record[field]
            elif fieldType["xcalarType"] == "DfArray":
                tmpRecord = record[field][-1]

        if appendMode:
            (fieldName, notUsed) = xpath[-1]
            tmpRecord[fieldName].append(fieldValue)
        else:
            tmpRecord[fieldName.strip('.')] = fieldValue

    def getFieldType(self, schema, xpath, fieldName):
        if len(xpath) > 0:
            previousType = xpath[-1][1]
            if previousType is None:
                previousType = xpath[-2][1]
            fieldType = previousType["children"][fieldName]
        else:
            fieldType = schema[fieldName]

        return fieldType

    def cast(self, fieldValue, fieldType):
        xcalarType = fieldType["xcalarType"]
        parquetType = fieldType["parquetType"]
        typeQualifier = fieldType["typeQualifier"]

        if parquetType == "int96":
            # Assume int96 is used for Hive TIMESTAMP data type.
            # This may not always be true!

            # TIMESTAMP is stored as 8 bytes representing nanoseconds
            # into a day, followed by 4 bytes representing the julian
            # day.
            dsBin = base64.b64decode(fieldValue)
            dayBin = dsBin[8:]
            nsBin = dsBin[:8]

            # 2440588 is the magic constant to convert from julian day
            # to unix epoch (1970-01-01) day.
            unixDay = int.from_bytes(dayBin, byteorder="little") - 2440588
            ns = int.from_bytes(nsBin, byteorder="little")

            # Ignore fractions of a second for now.
            uts = unixDay * 86400 + (ns / 1000000000)
            return uts

        elif isinstance(typeQualifier, list) and typeQualifier[0] == "DECIMAL":
            # This is a decimal field.  Convert to a string
            # representing the decimal number.
            s = base64.b64decode(fieldValue)
            numStr = str(int.from_bytes(s, byteorder="big"))
            leftShift = int(typeQualifier[2])
            length = len(numStr)
            if len(numStr[length - leftShift:]) == 0:
                return (numStr[0:length - leftShift] + ".0")
            else:
                return (numStr[0:length - leftShift] + "." +
                        numStr[length - leftShift:])

        elif xcalarType == "DfInt64" or xcalarType == "DfInt32":
            return int(fieldValue)
        elif xcalarType == "DfFloat64":
            return float(fieldValue)
        elif xcalarType == "DfString" and isinstance(fieldValue, bytes):
            return fieldValue.decode("UTF-8")
        elif xcalarType == "DfBoolean":
            if fieldValue.lower() == "false":
                return False
            else:
                return True

        return fieldValue

    def parseParquetBody(self,
                         stdout,
                         schema,
                         columns=None,
                         partitionKeys=None):
        record = {}
        xpath = []
        currObj = None
        for line in stdout:
            line = line.decode("UTF-8").strip()
            if line == "":
                if partitionKeys is not None:
                    for key in partitionKeys.keys():
                        if partitionKeys[key] == "__HIVE_DEFAULT_PARTITION__":
                            record[key] = None
                        else:
                            record[key] = partitionKeys[key]

                if columns is not None:
                    for column in columns:
                        tmpRecord = record
                        for token in self.split(column):
                            if token not in tmpRecord:
                                tmpRecord[token] = None
                                break
                            tmpRecord = tmpRecord[token]
                            if tmpRecord is None:
                                break
                yield record
                record = {}
                continue
            tokens = line.split(" = ")
            if len(tokens) == 2:
                (fieldName, fieldValue) = tokens
                numLeadingDots = len(fieldName) - len(fieldName.strip('.'))
                xpath = xpath[0:numLeadingDots]
                fieldType = self.getFieldType(schema, xpath,
                                              fieldName.strip('.'))
                self.insertValue(
                    record,
                    fieldName,
                    xpath,
                    self.cast(fieldValue, fieldType),
                    columns=columns)
            else:
                tokens = line.split(":")
                if len(tokens) == 2:
                    fieldName = tokens[0]
                    numLeadingDots = len(fieldName) - len(fieldName.strip('.'))
                    xpath = xpath[0:numLeadingDots]
                    strippedFieldName = fieldName.strip('.')

                    fieldType = None
                    if strippedFieldName == "bag" and len(xpath) > 0 and xpath[
                            -1][1]["xcalarType"] == "DfArray":
                        self.insertValue(
                            record,
                            fieldName,
                            xpath, {},
                            appendMode=True,
                            columns=columns)
                    else:
                        fieldType = self.getFieldType(schema, xpath,
                                                      strippedFieldName)
                        if fieldType["xcalarType"] == "DfArray":
                            self.insertValue(
                                record,
                                fieldName,
                                xpath, [],
                                appendMode=False,
                                columns=columns)
                        elif fieldType["xcalarType"] == "DfObject":
                            self.insertValue(
                                record,
                                fieldName,
                                xpath, {},
                                appendMode=False,
                                columns=columns)

                    xpath.append((strippedFieldName, fieldType))

    def convertToXcalarType(self, parquetType, typeQualifier):
        if parquetType == "binary" or parquetType == "string" or \
           (isinstance(typeQualifier, list) and typeQualifier[0] == "DECIMAL"):
            return "DfString"
        elif parquetType == "int64":
            return "DfInt64"
        elif parquetType == "int32":
            return "DfInt32"
        elif parquetType == "double" or parquetType == "int96" or parquetType == "float":
            # int96 is treated as TIMESTAMP. We return uts which is a double
            return "DfFloat64"
        elif parquetType == "group" and typeQualifier == "LIST":
            return "DfArray"
        elif parquetType == "group":
            return "DfObject"
        elif parquetType == "boolean" or parquetType == "bool":
            return "DfBoolean"
        else:
            return "DfUnknown"

    def processField(self, fieldIn, unknownFields):
        fieldOut = {
            "xcalarType":
                self.convertToXcalarType(fieldIn["typespec"],
                                         fieldIn.get("typeQualifier", None)),
            "parquetType":
                fieldIn["typespec"],
            "typeQualifier":
                fieldIn.get("typeQualifier", None)
        }

        if fieldOut["xcalarType"] == "DfUnknown":
            unknownFields.append(fieldIn)

        children = fieldIn.get("children", None)
        if children is None:
            return fieldOut
        fieldOut["children"] = {}
        for field in children:
            if fieldOut["xcalarType"] == "DfArray" and field[
                    "identifier"] == "bag":
                bagChild = field["children"][0]
                fieldOut["children"][
                    bagChild["identifier"]] = self.processField(
                        bagChild, unknownFields)
            else:
                fieldOut["children"][field["identifier"]] = self.processField(
                    field, unknownFields)

        return fieldOut

    def convertToXcalarSchema(self, parquetSchema, unknownFields):
        if len(parquetSchema) == 0:
            raise ParseException("parquetSchema is empty")
        xcalarSchema = {}
        for field in parquetSchema["body"]["fields"]:
            xcalarSchema[field["identifier"]] = self.processField(
                field, unknownFields)

        return xcalarSchema

    def split(self, string):
        strlen = len(string)
        start = 0
        cursor = 0
        while cursor < strlen:
            if string[cursor] == '\\':
                if cursor + 1 >= strlen:
                    yield string[start:cursor]
                    return
                cursor += 1
            elif string[cursor] == '.':
                yield string[start:cursor]
                start = cursor + 1
            cursor += 1
        yield string[start:cursor]

    def colInSchema(self, col, schema):
        currDict = schema
        for token in self.split(col):
            if currDict is None or token not in currDict:
                return False
            currDict = currDict[token].get("children", None)
        return True

    def containsValidColumns(self, columns, schema, partitionKeys):
        if partitionKeys is None:
            partitionKeys = {}

        for col in columns:
            if self.colInSchema(col, schema):
                return True

        return len(set(columns).intersection(set(partitionKeys.keys()))) > 0

    def isAvailable(self):
        xlrDir = os.environ.get("XLRDIR", "/opt/xcalar/")
        readerPath = os.path.join(xlrDir, "bin", "parquet-tools-1.8.2.jar")
        return os.path.isfile(readerPath)

    def parseParquet(self,
                     inputPath,
                     inputStream,
                     mode,
                     columns=None,
                     schema=None,
                     partitionKeys=None):
        xlrDir = os.environ.get("XLRDIR", "/opt/xcalar/")
        readerPath = os.path.join(xlrDir, "bin", "parquet-tools-1.8.2.jar")
        unknownFields = []
        assert inputPath is not None or inputStream is not None

        if inputPath is None:
            if inputStream is None:
                raise Exception("inputStream cannot be NULL")

            with tempfile.NamedTemporaryFile("wb") as tmpfile:
                shutil.copyfileobj(inputStream, tmpfile)
                tmpfile.flush()
                yield from self.parseParquet(tmpfile.name, None, mode, columns,
                                             schema, partitionKeys)
                return

        if mode != "cat" and mode != "schema" and mode != "catJson":
            raise NotImplementedError("Mode %s is not implemented" % mode)

        if mode == "cat" and schema is None:
            schema = self.convertToXcalarSchema(
                self.parseParquet(inputPath, None, "schema",
                                  columns).__next__(), unknownFields)
            if len(unknownFields) > 0:
                raise ParseException("Error processing schema: " +
                                     json.dumps(unknownFields))

        if mode == "catJson":
            command = ["java", "-jar", readerPath, "cat", "-j", inputPath]
        else:
            command = ["java", "-jar", readerPath, mode, inputPath]
        pr = subprocess.Popen(command, stdout=subprocess.PIPE)

        if mode == "catJson":
            for line in pr.stdout:
                rec = json.loads(line)
                yield rec
        elif mode == "cat":
            if columns is None or self.containsValidColumns(
                    columns, schema, partitionKeys):
                yield from self.parseParquetBody(pr.stdout, schema, columns,
                                                 partitionKeys)
        elif mode == "schema":
            try:
                yield self.parseParquetSchema(pr.stdout)
            except ppParseException as e:
                raise ParseException(e)
