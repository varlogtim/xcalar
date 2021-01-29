# PLEASE TAKE NOTE:
# UDFs can only support
# return values of type String

# Function names that start with __ are
# considered private functions and
# will not be directly invokable

import sys
import datetime
import time
import re
import hashlib
import json
import math
import codecs
import collections
from lxml import etree as ET
import xmltodict
import csv
import random

try:
    import xcalar.container.parquetUtils as parquetUtils
except ImportError:
    pass

# 3rd Party imports
try:
    import xlrd
except ImportError:
    pass

try:
    import dateutil.parser
except ImportError:
    pass

Utf8Reader = codecs.getreader("utf-8")


# ############ Import UDFs ###############
def __stripNull(s):
    # Helper function for other streaming UDFs
    # Call this with input being the string that you want to remove the null
    # character from
    return s.replace('\0', '')


def __cleanseColumnName(colName):
    colName = colName.strip()
    colName = re.sub(r'^[^a-zA-Z_]', "_", colName)
    colName = colName[0:256]
    if colName in ["DATA", "None", "True", "False"]:
        colName = "_" + colName
    colName = re.sub(r"[\^'\"\(\)\[\]\{\}\\:,\.]", "_", colName)
    return colName


def __conformHeaders(headerArray):
    newNames = []
    final = []
    change = []
    noColl = {}
    for header in headerArray:
        header = __cleanseColumnName(header)
        newNames.append(header)
    for idx, header in enumerate(newNames):
        if header not in noColl:
            if header == headerArray[idx]:
                final.append((idx, header))
                noColl[header] = True
            else:
                change.append((idx, header))
        else:
            change.append((idx, header))
    for idx, header in change:
        if header in noColl:
            found = False
            for suff in range(1000):
                trial = header + "_" + str(suff)
                if (trial not in noColl):
                    found = True
                    break
            if (found):
                header = trial
            else:
                header = header + "_" + str(random.randint(10000, 99999))
        final.append((idx, header))
        noColl[header] = True
    final.sort(key=lambda x: x[0])
    _, newNames = zip(*final)
    return list(newNames)


def __detectFormat(column, confidenceRate=1):
    intHit = 0
    floatHit = 0
    boolHit = 0
    validData = 0
    for val in column:
        val = val.strip().lower()
        if val == "":
            continue

        validData += 1
        guess = None
        try:
            value = int(val)
            guess = int
            intHit += 1
            floatHit += 1
        except ValueError:
            pass
        if not guess:
            try:
                value = float(val)
                guess = float
                floatHit += 1
            except ValueError:
                pass
        if not guess:
            if (val.lower() == "true" or val.lower() == "false"):
                guess = bool
                boolHit += 1
        if not guess:
            guess = str
    if validData == 0:
        return str
    elif intHit / validData >= confidenceRate:
        return int
    elif floatHit / validData >= confidenceRate:
        return float
    elif boolHit / validData >= confidenceRate:
        return bool
    else:
        return str


def standardizeColumnNamesAndTypes(fullPath,
                                   inStream,
                                   withHeader=False,
                                   skipRows=0,
                                   field="\t",
                                   record="\n",
                                   quote='"',
                                   allowMixed=True):
    # Set dialect according user's preferences
    dialect = csv.Dialect
    dialect.delimiter = field
    dialect.escapechar = "\\"
    dialect.lineterminator = record
    if len(quote) == 0:
        dialect.quoting = csv.QUOTE_NONE
    else:
        dialect.quoting = csv.QUOTE_MINIMAL
        dialect.doublequote = False
        dialect.quotechar = quote
    csv.register_dialect("userCsv", dialect)

    # Can be configured
    numLines = 40
    confidence = 0.9

    # Read data in and set headers
    firstLine = True
    maxFields = 0
    dataMatrix = []
    data = []

    lineCount = 0
    for line in csv.reader(Utf8Reader(inStream), dialect="userCsv"):
        if firstLine and withHeader:
            firstLine = False
            headers = line
            continue
        if maxFields < len(line):
            maxFields = len(line)
        dataMatrix.append(line)
        lineCount += 1
        if (lineCount < numLines):
            dataMatrix.append(line)
        data.append(line)

    # Handle empty files
    numDataLines = lineCount
    if (numDataLines <= 0):
        return

    if not withHeader:
        headers = ["column" + str(i) for i in range(maxFields)]

    # Pad headers
    if len(headers) < maxFields:
        headers += [
            "column" + str(i + len(headers))
            for i in range(maxFields - len(headers))
        ]

    # Clean headers
    newHeaders = __conformHeaders(headers)

    # Pad empties
    for row in dataMatrix:
        if len(row) < maxFields:
            row += [""] * (maxFields - len(row))

    meta = []
    for col in range(maxFields):
        colData = [r[col] for r in dataMatrix]
        detectedType = __detectFormat(colData, confidence)
        meta.append((newHeaders[col], detectedType))

    for row in data:
        record = {}
        for idx, val in enumerate(row):
            (colName, colType) = meta[idx]
            value = val

            if colType == "bool":
                if val.lower().strip() == "true":
                    value = True
                elif val.lower().strip() == "false":
                    value = False
                else:
                    if not allowMixed:
                        value = False
            else:
                try:
                    value = colType(val)
                except ValueError:
                    if not allowMixed:
                        if colType == int:
                            value = 0
                        elif colType == float:
                            value = float(0)
            record[colName] = value
        yield record


def openExcel(fullPath, inStream, withHeader=False, skipRows=0, sheetIndex=0):
    fullFile = inStream.read()
    xl_workbook = xlrd.open_workbook(file_contents=fullFile)
    xl_sheet = xl_workbook.sheet_by_index(sheetIndex)
    num_cols = xl_sheet.ncols    # Number of columns
    headers = []
    for row_idx in range(skipRows, xl_sheet.nrows):    # Iterate through rows
        row = collections.OrderedDict()
        for col_idx in range(0, num_cols):    # Iterate through columns
            val = xl_sheet.cell_value(row_idx,
                                      col_idx)    # Get cell object by row, col
            cell_type = xl_sheet.cell_type(row_idx, col_idx)
            if cell_type == xlrd.XL_CELL_DATE:
                val = "%s,%s" % (val, xl_workbook.datemode)
            elif cell_type == xlrd.XL_CELL_NUMBER:
                pass
            elif cell_type == xlrd.XL_CELL_BOOLEAN:
                val = bool(val)
            else:
                val = "%s" % val
            if withHeader:
                if row_idx == skipRows:
                    if val == "":
                        headers.append('column{}'.format(col_idx))
                    else:
                        headers.append(str(val))
                    if col_idx == num_cols - 1:
                        # all column names have been added to headers array
                        headers = __conformHeaders(headers)
                else:
                    row[headers[col_idx]] = val
            else:
                row['column{}'.format(col_idx)] = val
        if not withHeader or row_idx > skipRows:
            yield row


def xmlToJson(fullPath, xmlStream, xPath="", withPath=True, matchedPath=True):
    # xPath is a comma delimited string of different xPaths
    utf8Stream = Utf8Reader(xmlStream)
    tree = ET.parse(utf8Stream)

    allPaths = [x.strip() for x in xPath.split(",")]
    root = tree.getroot()
    tree = ET.ElementTree(root)
    for path in allPaths:
        elems = root.xpath(path)
        if (type(elems) == bool) or (type(elems) == float) or (
                type(elems) == str):
            p = tree.getpath(elems)
            record = collections.OrderedDict()
            record["xcError"] = True
            record[
                "xcErrorMessage"] = "XPath does not point to an element. Instead it is a: " + type(
                    elems).__name__
            record["xcElementXPath"] = p
            record["xcMatchedXPath"] = path
            yield record
        elif (type(elems) == list):
            for e in elems:
                d = xmltodict.parse(ET.tostring(e))
                if withPath:
                    d["xcElementXpath"] = tree.getpath(e)
                if matchedPath:
                    d["xcMatchedXpath"] = path
                yield d


def convertNewLineJsonToArrayJson(fullPath, inStream):
    utf8Stream = Utf8Reader(inStream)
    for line in utf8Stream:
        line = line.strip()
        if not line:
            continue
        if line[-1] == ',':
            line = line[:-1]
        line = __stripNull(line)
        d = json.loads(line, object_pairs_hook=collections.OrderedDict)
        yield d


# Function will be removed which feature is supported natively
def genLineNumber(fullPath, inStream, header=False):
    utf8Stream = Utf8Reader(inStream)
    lineNo = 1
    if header:
        title = __stripNull(utf8Stream.readline().strip())
    for line in utf8Stream:
        line = __stripNull(line)
        d = {"lineNumber": lineNo}
        if header:
            d[title] = line
        else:
            d["lineContents"] = line
        yield d
        lineNo += 1


# ############# User Common Map UDF Functions #################
# Below are functions that we expect users to use
# get the current time
def now():
    return str(int(time.time()))


def md5sum(col):
    return hashlib.md5(col.encode("utf8")).hexdigest()


def logBuckets(n):
    if n >= 0 and n < 1:
        return 0
    elif n < 0 and n >= -1:
        return -1
    elif n < 0:
        res = math.ceil(math.log(abs(n), 10)) + 1
        return -1 * int(res)
    else:
        # to fix the inaccuracy of decimal, example, log(1000, 10) = 2.9999999999999996
        res = math.floor(math.log(n, 10) + 0.0000000001) + 1
        return int(res)


def coalesce(*args):
    for a in args:
        if a is not None:
            return a
    return None


def convertExcelTime(colName, outputFormat):
    (val, datemode) = colName.split(",")
    if (not val or not datemode):
        return "Your input must be val,datemode"
    (y, mon, d, h, m, s) = xlrd.xldate_as_tuple(float(val), int(datemode))
    return str(datetime.datetime(y, mon, d, h, m, s).strftime(outputFormat))


# get the substring of txt after the (index)th delimiter
# for example, splitWithDelim("a-b-c", 1, "-") gives "b-c"
# and splitWithDelim("a-b-c", 3, "-") gives ""
def splitWithDelim(txt, index, delim):
    return delim.join(txt.split(delim)[index:])


# %a    Locale's abbreviated weekday name.
# %A    Locale's full weekday name.
# %b    Locale's abbreviated month name.
# %B    Locale's full month name.
# %c    Locale's appropriate date and time representation.
# %d    Day of the month as a decimal number [01,31].
# %H    Hour (24-hour clock) as a decimal number [00,23].
# %I    Hour (12-hour clock) as a decimal number [01,12].
# %j    Day of the year as a decimal number [001,366].
# %m    Month as a decimal number [01,12].
# %M    Minute as a decimal number [00,59].
# %p    Locale's equivalent of either AM or PM. (1)
# %S    Second as a decimal number [00,61]. (2)
# %U    Week number of the year (Sunday as the first day of the week) as a decimal number [00,53]. All days in a new year preceding the first Sunday are considered to be in week 0.    (3)
# %w    Weekday as a decimal number [0(Sunday),6].
# %W    Week number of the year (Monday as the first day of the week) as a decimal number [00,53]. All days in a new year preceding the first Monday are considered to be in week 0.    (3)
# %x    Locale's appropriate date representation.
# %X    Locale's appropriate time representation.
# %y    Year without century as a decimal number [00,99].
# %Y    Year with century as a decimal number.
# %Z    Time zone name (no characters if no time zone exists).
# %%    A literal '%' character.


def convertFormats(colName, outputFormat, inputFormat=None):
    if inputFormat is None or inputFormat == "":
        try:
            timeStruct = dateutil.parser.parse(colName).timetuple()
            outString = time.strftime(outputFormat, timeStruct)
            return outString
        except ValueError:
            return "Unable to detect, please retry specifying inputFormat"
        except Exception:
            return "%s %s" % ("Unexpected error", sys.exc_info()[0])
    else:
        timeStruct = time.strptime(colName, inputFormat)
        outString = time.strftime(outputFormat, timeStruct)
        return outString


def convertFromUnixTS(colName, outputFormat):
    return datetime.datetime.fromtimestamp(
        float(colName)).strftime(outputFormat)


def convertToUnixTS(colName, inputFormat=None):
    if inputFormat is None or inputFormat == "":
        try:
            return str(
                float(time.mktime(dateutil.parser.parse(colName).timetuple())))
        except ValueError:
            return "Unable to detect, please retry specifying inputFormat"
        except Exception:
            return "%s %s" % ("Unexpected error", sys.exc_info()[0])
    return str(
        time.mktime(
            datetime.datetime.strptime(colName, inputFormat).timetuple()))


# ############# Unimplemented #################
def parseCsv():
    raise NotImplementedError()


def parseJson():
    raise NotImplementedError()


# DEPRECATED DO NOT USE. USING THESE FUNCTIONS WILL BREAK YOUR WORKBOOK
def multiJoin(*arg):
    stri = ""
    for a in arg:
        stri = stri + str(a) + ".Xc."
    return stri


def openExcelWithHeader(fullPath, inStream):
    for row in openExcel(fullPath, inStream, withHeader=True):
        yield row


def genLineNumberWithHeader(fullPath, inStream):
    for row in genLineNumber(fullPath, inStream, header=True):
        yield row


# Parse parquet using the official parquet-tools utility to conver to JSON.
# Be careful using this, because often the backing parquet files for a given
# storage solution will be significantly different than what the storage
# solution presents to the user.
def parseParquet(inputPath,
                 inputStream,
                 mode="cat",
                 columns=None,
                 parquetParser=None):
    for line in parquetUtils.parseParquet(
            inputPath, inputStream, mode, columns,
            parquetParserIn=parquetParser):
        yield line


# Normalizes values to be on a scale from 0 to 1
# Requires user to input the colName and min/max column values
# Optional parameter numRows is used if minValue == maxValue (all values in column are the same)
def normalize(colName, minValue, maxValue, numRows=None):
    try:
        if minValue < maxValue:
            return (colName - minValue) / (maxValue - minValue)
        elif minValue > maxValue:
            raise ValueError(
                "Unable to normalize value, minValue must be less than maxValue"
            )
        elif minValue == maxValue and numRows is not None:
            return (1 / numRows)
        raise ValueError("Unable to normalize value")
    except Exception as e:
        return ("Error {}".format(repr(e)))
