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
from urlparse import urlparse, urlunparse

# 3rd Party imports
try:
    import xlrd
except:
    pass

try:
    import pytz
except:
    pass

try:
    import dateutil.parser
except:
    pass

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
    if inputFormat == None or inputFormat == "":
        try:
            timeStruct = dateutil.parser.parse(colName).timetuple()
            outString = time.strftime(outputFormat, timeStruct)
            return outString
        except ValueError:
            return "Unable to detect, please retry specifying outputFormat"
        except:
            return "%s %s" % ("Unexpected error", sys.exc_info()[0])
    else:
        timeStruct = time.strptime(colName, inputFormat)
        outString = time.strftime(outputFormat, timeStruct)
        return outString

def convertFromUnixTS(colName, outputFormat):
    return datetime.datetime.fromtimestamp(float(colName)).strftime(outputFormat)

def convertToUnixTS(colName, inputFormat=None):
    if inputFormat == None or inputFormat == "":
        try:
            return str(float(time.mktime(dateutil.parser.parse(colName).timetuple())))
        except ValueError:
            return "Unable to detect, please retry specifying inputFormat"
        except:
            return "%s %s" % ("Unexpected error", sys.exc_info()[0])
    return str(time.mktime(datetime.datetime.strptime(colName, inputFormat).timetuple()))

def __excelHelper(fullPath, inStream, withHeader):
    fullFile = inStream.read()
    xl_workbook = xlrd.open_workbook(file_contents=fullFile)
    xl_sheet = xl_workbook.sheet_by_index(0)
    num_cols = xl_sheet.ncols   # Number of columns
    headers = []
    for row_idx in range(0, xl_sheet.nrows):    # Iterate through rows
        row = {}
        for col_idx in range(0, num_cols):  # Iterate through columns
            val = xl_sheet.cell_value(row_idx, col_idx)  # Get cell object by row, col
            if xl_sheet.cell_type(row_idx, col_idx) == xlrd.XL_CELL_DATE:
                val = "%s,%s" % (val, xl_workbook.datemode)
            else:
                val = "%s" % val
            if withHeader:
                if row_idx == 0:
                    headers.append(val)
                else:
                    row[headers[col_idx]] = val
            else:
                row['column{}'.format(col_idx)] = val
        if not withHeader or row_idx > 0:
            yield row

def openExcel(fullPath, inStream):
    for row in __excelHelper(fullPath, inStream, False):
        yield row

def openExcelWithHeader(fullPath, inStream):
    for row in __excelHelper(fullPath, inStream, True):
        yield row

def convertExcelTime(colName, outputFormat):
    (val, datemode) = colName.split(",")
    if (not val or not datemode):
        return "Your input must be val,datemode"
    (y, mon, d, h, m, s) = xlrd.xldate_as_tuple(float(val), int(datemode))
    return str(datetime.datetime(y, mon, d, h, m, s).strftime(outputFormat))

# get the substring of txt after the (index)th delimiter
# for example, splitWithDelim("a-b-c", "-", 1) gives "b-c"
# and splitWithDelim("a-b-c", "-", 3) gives ""
def splitWithDelim(txt, index, delim):
    return delim.join(txt.split(delim)[index:])

# used for multijoin and multiGroupby
def multiJoin(*arg):
    stri = ""
    for a in arg:
        stri = stri + str(a) + ".Xc."
    return stri

def convertNewLineJsonToArrayJson(inpath, inStream):
    for line in inStream:
        line = line.strip()
        if not line:
            continue
        if line.rfind(",") == len(line) - 1:
            line = line[:-1]
        d = json.loads(line)
        yield d

# Below are functions that we expect users to use
# get the current time
def now():
    return str(int(time.time()))

def md5sum(col):
    return hashlib.md5(col).hexdigest()
