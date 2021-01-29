# Copyright 2017 - 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import json
import tempfile
import math
import random
import string
from itertools import groupby

import pytest
import time
from datetime import datetime, timezone
from dateutil import parser

from xcalar.compute.util.Qa import DefaultTargetName, datasetCheck
from xcalar.external.client import Client

from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiException
from xcalar.external.LegacyApi.Dataset import JsonDataset
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.result_set import ResultSet

from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingT
from xcalar.compute.coretypes.Status.ttypes import StatusT

testXDFsSessionName = "TestXDFs"

# Make these tests random, but deterministic
seed = 12341234
random.seed(seed)
# This is used for some tests where the records are generated
numRecords = 1001


def randomString(length):
    return str(''.join(
        random.choice(string.ascii_uppercase + string.digits)
        for _ in range(length)))


def makeUnaryMathTest(func, minVal=-10**9, maxVal=10**9):
    funcName = func.__name__
    testCases = []
    for _ in range(numRecords):
        inputVal = random.uniform(minVal, maxVal)
        outVal = func(inputVal)
        testCases.append((inputVal, outVal))

    return ("{}(p::column0)".format(funcName), testCases)


def randomRangeCheck(resultSet):
    for row in resultSet:
        assert row["mappedField"] in list(range(1, 4))
    return 1


def nowChecker(resultSet):
    now = datetime.now(timezone.utc)

    for row in resultSet:
        then = parser.parse(row["mappedField"])
        delta = now - then
        assert abs(delta.seconds) < 10
    return 1


def todayChecker(resultSet):
    today = datetime.now(timezone.utc).date()
    for row in resultSet:
        then = parser.parse(row["mappedField"]).date()
        delta = today - then
        print(delta)
        assert delta.days == 0
    return 1


# Type is: [xdfNegativeSuite]
# xdfNegativeSuite := (xdfName, [case], expectedErrorStatus)
# case := (arg0, arg1, ...)

# Type is: [xdfSuite]
# xdfSuite := (xdfName, [case], verificationFunction(optional))
# case := (arg0, arg1, ..., expectedResult)
# 'None' is used to represent an error row. This means that XDFs which output
# an actual 'null' are currently unrepresentable; this is fine for now, since
# no XDFs output null
#
# We split up the tests into sections because otherwise the code formatter
# Thinks the potential lines are too long and gives up. We also may want to
# eventually treat these tests differently at some point.
xdfTests = [
    (
        "strip(p::column0)",
        [
            ("1", "1"),
            (" 1  ", "1"),
            ("12  ", "12"),
            ("  221", "221"),
        ],
    ),
    (
        "strip(p::column0, p::column1)",
        [("1", "|", "1"), ("|1||", "|", "1"), ("1||", "|", "1"),
         ("|||1", "|", "1"), ("   3", None, "3"), (None, None, None)],
    ),
    (
        "abs(p::column0)",
        [
            (1, 1.0),
            (-1, 1.0),
            (12.7, 12.7),
            (-12.7, 12.7),
            (0, 0),
        ],
    ),
    (
        "absInt(p::column0)",
        [
            (1, 1),
            (-1, 1),
            (0, 0),
        ],
    ),
    (
        "absNumeric(p::column0)",
        [
            (1, '1.00'),
            (-1, '1.00'),
            (0, '0.00'),
        ],
    ),
    (
        "absNumeric(money(p::column0))",
        [
            ('12.70', '12.70'),
            ('-12.78', '12.78'),
            ('-12.7', '12.70'),
            ('-12.788', '12.79'),
        ],
    ),
    (
        "stripLeft(p::column0)",
        [
            ("1", "1"),
            (" 1  ", "1  "),
            ("12  ", "12  "),
            ("  221", "221"),
        ],
    ),
    (
        "stripLeft(p::column0, p::column1)",
        [("1", "|", "1"), ("|1||", "|", "1||"), ("1||", "|", "1||"),
         ("|||1", "|", "1"), ("   3", None, "3"), (None, None, None)],
    ),
    (
        "stripRight(p::column0)",
        [
            ("1", "1"),
            (" 1  ", " 1"),
            ("12  ", "12"),
            ("  221", "  221"),
        ],
    ),
    (
        "stripRight(p::column0, p::column1)",
        [("1", "|", "1"), ("|1||", "|", "|1"), ("1||", "|", "1"),
         ("|||1", "|", "|||1"), ("3   ", None, "3"), (None, None, None)],
    ),
    (
        "in(p::column0, p::column1)",
        [
            (1, 1, True),
            (1, 2, False),
            ("1", "2", False),
            ("1", "1", True),
        ],
    ),
    (
        "in(p::column0, p::column1, p::column2, p::column3)",
        [
            (1, 1, 2, 3, True),
            (1, 2, 3, 4, False),
            ("1", "2", "3", "4", False),
            ("1", "2", "3", "1", True),
        ],
    ),
    (
        "concatDelim(p::column0, p::column1, p::column2, p::column3, p::column4)",
        [("|", "null", True, "abc", "123", "abc|123"),
         (",", "abc", True, "a", None, "a,abc"),
         ("ab", "null", True, None, "b", "nullabb"),
         ("&", "FNF", True, None, None, "FNF&FNF"),
         ("&", "FNF", False, None, None, ""),
         ("ab", "null", False, None, "b", "b"),
         ("|", "null", False, "abc", "123", "abc|123")],
    ),
    (
        "stringsPosCompare(p::column0, p::column1, p::column2, p::column3,p::column4)",
        [
            (",,,,", ",,,,", ",", 0, 1, True),
            ("1,2,1,,", "1,,1,,", ",", 1, 2, True),
            ("1,1,2,2,3", ",,,,", ",", 2, 3, False),
            ("1,2,3", "1,2,3", ",", 1, 2, False),
            ("1,2,3", "1,2,3", ",", 0, 1, True),
            ("a12b12c12d", "a1212c12d", "12", 1, 2, True),
            ("a12b12c12d", "a1212c12d", "12", 0, 1, False),
        ],
    ),
    ("genRandom(p::column0, p::column1)", [
        (1, 3, 1),
        (1, 3, 1),
        (1, 3, 1),
        (1, 3, 1),
        (1, 3, 1),
        (1, 3, 1),
        (1, 3, 1),
        (1, 3, 1),
        (1, 3, 1),
        (1, 3, 1),
    ], randomRangeCheck),
]

# Date/time tests
# First calculate the local timezone offset.  This is important, since
# a timestamp without a timezone is implicitly declared as local time
# under ISO8601.  We can't hardcode this either, because this offset
# changes with Daylight Savings Time.
tzoff = "{:02d}:00".format(abs(int(time.timezone / 3600)))
if time.timezone > 0:
    tzoff = "-" + tzoff
elif time.timezone < 0:
    tzoff = "+" + tzoff
else:
    tzoff = "Z"

xdfTests.extend([
    (
        "timestamp(p::column0)",
        [
            (1, "1970-01-01T00:00:00.001Z"),
            (2.0, "1970-01-01T00:00:00.002Z"),
            (2.5, "1970-01-01T00:00:00.002Z"),
            (-1, "1969-12-31T23:59:59.999Z"),
            (-1000001, "1969-12-31T23:43:19.999Z"),
            (-1000000000001, "1938-04-24T22:13:19.999Z"),
            ("2018-01-01", "2018-01-01T00:00:00.000Z"),
            ("20180101", "2018-01-01T00:00:00.000Z"),
            ("2018-01-01T01:01:01Z", "2018-01-01T01:01:01.000Z"),
            ("20180101T01:01:01Z", "2018-01-01T01:01:01.000Z"),
            ("20180101T010101Z", "2018-01-01T01:01:01.000Z"),
            ("20180101T010101", "2018-01-01T01:01:01.000" + tzoff),
            ("2018-01-01 01:01:01Z", "2018-01-01T01:01:01.000Z"),
            ("2018-01-01 01:01:01", "2018-01-01T01:01:01.000" + tzoff),
            (1514768461000, "2018-01-01T01:01:01.000Z"),
            ("2018-01-01T01:01:01.5Z", "2018-01-01T01:01:01.500Z"),
            ("2018-01-01 01:01:01.111111Z", "2018-01-01T01:01:01.111Z"),
            ("2018-01-01 01:01:01.123", "2018-01-01T01:01:01.123" + tzoff),
            (253396947661020, "9999-10-31T01:01:01.020Z"),
            ("1969-10-31T01:01:01Z", "1969-10-31T01:01:01.000Z"),
            ("1969-10-31T01:01:01.001Z", "1969-10-31T01:01:01.001Z"),
            ("9999-10-31T01:01:01.121+05:00", "9999-10-31T01:01:01.121+05:00"),
            ("1969-10-31T01:01:01.5555+06:00",
             "1969-10-31T01:01:01.555+06:00"),
            ("1969-10-31T01:01:01.001-01:00", "1969-10-31T01:01:01.001-01:00"),
            ("1969-10-31T01:01:01.001-0700", "1969-10-31T01:01:01.001-07:00"),
            ("1969-10-31T01:01:01.001-07", "1969-10-31T01:01:01.001-07:00"),
            ("19691031 010101.001-07", "1969-10-31T01:01:01.001-07:00"),
            ("19691031 010101.001U", "1969-10-31T01:01:01.001-08:00"),
            ("19691031 010101.001A", "1969-10-31T01:01:01.001+01:00"),
            ("19691031Z111518.001B", "1969-10-31T00:00:00.000Z"),
            ("9990712", None),
            ("19691031T010101.001J", None),
            ("2018:01:01", None),
            ("T19691031", None),
            ("100000712", None),
            ("19700101 but is way too long to contain a valid timestamp so it will return none",
             None),
            ("A string without a timestamp", None),
            ("19720712TXXX", None),
            ("20120703T08", None),
            ("1972-07-12T00:00:00+0930",
             None),    # Fractional timestamps unsupported
            ("1972-07-12T00:00:00+09:30",
             None),    # Fractional timestamps unsupported
        ]),
    (
        "convertTimezone(timestamp(p::column0), p::column1)",
        [
            ("9999-10-31T20:01:01.121-07:00", 0, "9999-11-01T03:01:01.121Z"),
            ("9999-10-31T01:01:01.121+05:00", 0, "9999-10-30T20:01:01.121Z"),
            ("9999-10-31T01:01:01.121Z", 5, "9999-10-31T06:01:01.121+05:00"),
            ("9999-10-31T01:01:01.121Z", -7, "9999-10-30T18:01:01.121-07:00"),
        ],
    ),
    (
        "addDateInterval(timestamp(p::column0), p::column1, p::column2, p::column3)",
        [
            (1, 0, 1, 0, "1970-02-01T00:00:00.001Z"),
            (-1, 0, -1, 0, "1969-11-30T23:59:59.999Z"),
            (1, 0, 0, 1, "1970-01-02T00:00:00.001Z"),
            (-1, 0, 0, -1, "1969-12-30T23:59:59.999Z"),
            (1, 1, 0, 0, "1971-01-01T00:00:00.001Z"),
            (-1, -1, 0, 0, "1968-12-31T23:59:59.999Z"),
            (1, 1, 1, -1, "1971-01-31T00:00:00.001Z"),
            (-1, -1, -1, 1, "1968-12-01T23:59:59.999Z"),
        ],
    ),
    (
        "addTimeInterval(timestamp(p::column0), p::column1, p::column2, p::column3)",
        [
            (1, 0, 1, 0, "1970-01-01T00:01:00.001Z"),
            (-1, 0, -1, 0, "1969-12-31T23:58:59.999Z"),
            (1, 0, 0, 1, "1970-01-01T00:00:01.001Z"),
            (-1, 0, 0, -1, "1969-12-31T23:59:58.999Z"),
            (1, 1, 0, 0, "1970-01-01T01:00:00.001Z"),
            (-1, -1, 0, 0, "1969-12-31T22:59:59.999Z"),
            (1, 1, 1, -1, "1970-01-01T01:00:59.001Z"),
            (-1, -1, -1, 1, "1969-12-31T22:59:00.999Z"),
            ("2000-03-01T01:01:01.001+08:00", 1, 0, 0,
             "2000-03-01T02:01:01.001+08:00"),
        ],
    ),
    (
        "addIntervalString(timestamp(p::column0), p::column1)",
        [
            (1, "1,1,-1", "1971-01-31T00:00:00.001Z"),
            (-1, "-1,-1,1", "1968-12-01T23:59:59.999Z"),
            (1, "0,0,0,1,1,-1", "1970-01-01T01:00:59.001Z"),
            (-1, "0,0,0,-1,-1,1", "1969-12-31T22:59:00.999Z"),
            (1, "1,1,-1,24,60,61.1", "1971-02-01T01:01:01.101Z"),
            ("2000-02-29T01:01:01.001+08:00", "0,0,1,1",
             "2000-03-01T02:01:01.001+08:00"),
        ],
    ),
    (
        "datePart(timestamp(p::column0), p::column1)",
        [
            ("1969-02-01T00:00:00.001Z", "Y", 1969),
            ("2018-02-01T00:00:00.001Z", "Y", 2018),
            ("2018-09-01T00:00:00.001Z", "Q", 3),
            ("2018-02-01T00:00:00.001Z", "M", 2),
            ("2018-02-01T00:00:00.001Z", "D", 1),
            ("2018-09-11T00:00:00.001Z", "W", 3),
            ("2000-03-01T01:01:01.001+08:00", "M", 3),
        ],
    ),
    (
        "timePart(timestamp(p::column0), p::column1)",
        [
            ("1969-02-01T01:01:01.001Z", "H", 1),
            ("1970-02-01T01:01:01.001Z", "M", 1),
            ("1971-02-01T01:01:01.001Z", "S", 1),
            ("2000-03-01T01:01:01.001+08:00", "H", 1),
        ],
    ),
    (
        "dateDiff(timestamp(p::column0), timestamp(p::column1))",
        [
            ("1970-02-01T00:00:00.001Z", "1970-01-31T10:00:00.001Z", -1),
            ("1970-01-31T10:10:00.002Z", "1970-02-01T00:00:00.001Z", 1),
            ("1970-02-01T00:00:00.001Z", "1970-02-01T10:00:00.001Z", 0),
        ],
    ),
    (
        "dayOfYear(timestamp(p::column0))",
        [
            ("1969-02-01T01:01:01.001Z", 32),
            ("1969-02-01T01:01:01.001Z", 32),
            ("2000-01-01T01:01:01.001+08:00", 1),
        ],
    ),
    (
        "lastDayOfMonth(timestamp(p::column0))",
        [("2018-09-11T01:01:01.001Z", "2018-09-30T01:01:01.001Z"),
         ("2018-10-11T01:01:01.001Z", "2018-10-31T01:01:01.001Z"),
         ("2018-02-28T01:01:01.001Z", "2018-02-28T01:01:01.001Z"),
         ("2004-02-28T01:01:01.001Z", "2004-02-29T01:01:01.001Z"),
         ("2000-02-28T01:01:01.001Z", "2000-02-29T01:01:01.001Z"),
         ("2100-02-28T01:01:01.001Z", "2100-02-28T01:01:01.001Z"),
         ("2000-03-01T01:01:01.001+03:00", "2000-03-31T01:01:01.001+03:00")],
    ),
    (
        "dateTrunc(timestamp(p::column0), p::column1)",
        [
            (0, "yEaR", "1970-01-01T00:00:00.000Z"),
            (0, "yy", "1970-01-01T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "YYYY", "2018-01-01T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "quarter",
             "2018-07-01T00:00:00.000Z"),
            (0, "Month", "1970-01-01T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "mon", "2018-09-01T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "mm", "2018-09-01T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "week", "2018-09-10T00:00:00.000Z"),
            (0, "day", "1970-01-01T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "dd", "2018-09-14T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "hour", "2018-09-14T13:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "minute", "2018-09-14T13:31:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "second", "2018-09-14T13:31:05.000Z"),
            ("2000-01-01 03:31:05.000+08:00", "YEAR",
             "2000-01-01T00:00:00.000+08:00"),
        ],
    ),
    (
        "monthsBetween(timestamp(p::column0), timestamp(p::column1))",
        [
            ("2018-09-14 13:31:05.000Z", "2018-09-14 13:31:05.000Z", 0),
            ("2018-03-31 13:31:05.000Z", "2018-02-28 13:31:05.000Z", 1.0),
            ("2018-02-28 13:31:05.000Z", "2018-03-28 13:31:05.000Z", -1.0),
        ],
    ),
    (
        "nextDay(timestamp(p::column0), p::column1)",
        [
            ("2018-09-14 13:31:05.000Z", "mO", "2018-09-17T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "tu", "2018-09-18T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "we", "2018-09-19T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "TH", "2018-09-20T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "fr", "2018-09-21T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "sa", "2018-09-15T00:00:00.000Z"),
            ("2018-09-14 13:31:05.000Z", "su", "2018-09-16T00:00:00.000Z"),
        ],
    ),
    (
        "weekOfYear(timestamp(p::column0))",
        [
            ("2018-09-14T01:01:01.001Z", 37),
            (0, 1),
        ],
    ),
    (
        "dateDiffDay(p::column0, p::column1, p::column2)",
        [
            ("2012-01-01", "2012-01-01", "%F", 0),
            ("2012-01-01", "2012-01-02", "%F", 1),
            ("2012-01-01", "2012-02-01", "%F", 31),
            ("2012-01-01", "2012-03-01", "%F", 60),
            ("2012-01-01", "2013-01-01", "%F", 366),
            ("2014-01-01", "2013-01-01", "%F", -365),
        ],
    ),
    ("between(timestamp(p::column0), timestamp(p::column1), timestamp(p::column2))",
     [
         ("1972-07-12T00:00:00", "1970-01-01T00:00:00", "9999-12-31T23:59:59",
          True),
         ("1972-07-12T00:00:00", "9999-12-31T23:59:59", "1970-01-01T00:00:00",
          False),
         ("1972-07-12T00:00:00", "1972-07-12T00:00:00", "1970-01-01T00:00:00",
          False),
         ("1972-07-12T00:00:00", "1970-01-01T00:00:00", "1972-07-12T00:00:00",
          True),
         ("1972-07-12T00:00:00", "1972-07-12T00:00:00", "1972-07-12T00:00:00",
          True),
         ("1972-07-12T00:00:00", "1972-07-13T00:00:00", "1972-07-13T00:00:00",
          False),
         ("2018-01-01T00:00:00-04:00", "2018-01-01T00:00:00-08:00",
          "2018-01-01T00:00:00Z", False),
         ("2018-01-01T00:00:00-04:00", "2018-01-01T00:00:00Z",
          "2018-01-01T00:00:00-08:00", True),
     ]),
    ("now()", [(1, )], nowChecker),
    ("today()", [(1, )], todayChecker),
])

# Math tests
xdfTests.extend([
    (
        "pi()",
        [
            (math.pi, ),
        ],
    ),
    (
        "round(p::column0)",
        [
            (1.1, 1.0),
            (1.5, 2.0),
            (1.51111, 2.0),
        ],
    ),
    (
        "round(p::column0, p::column1)",
        [
            (1.1, 2, 1.10),
            (1.55, 1, 1.6),
            (1.51111, 3, 1.511),
        ],
    ),
    (
        "add(p::column0, p::column1, p::column2)",
        [
            (1, 1, 1, 3.0),
        ],
    ),
    (
        "add(p::column0, p::column1, p::column2, p::column3)",
        [
            (1, 1, 1, -1, 2.0),
        ],
    ),
    (
        "addInteger(p::column0, p::column1, p::column2)",
        [
            (1.5, 1.9, 1, 3),
        ],
    ),
    (
        "sub(p::column0, p::column1, p::column2)",
        [
            (3, 1, 1, 1.0),
        ],
    ),
    (
        "sub(p::column0, p::column1, p::column2, p::column3)",
        [
            (4, 1, 1, -1, 3.0),
        ],
    ),
    (
        "subInteger(p::column0, p::column1, p::column2)",
        [
            (3, 1, 1.7, 1),
        ],
    ),
    (
        "mult(p::column0, p::column1, p::column2)",
        [
            (3, 1, 2, 6.0),
        ],
    ),
    (
        "mult(p::column0, p::column1, p::column2, p::column3)",
        [
            (4, 1, 1, -1, -4.0),
        ],
    ),
    (
        "multInteger(p::column0, p::column1, p::column2)",
        [
            (3, 1, 2.5, 6),
        ],
    ),
    (
        "addNumeric(p::column0, p::column1, p::column2)",
        [
            (1, 2, 3, '6.00'),
            (-1, -2, -3, '-6.00'),
            ('1', '2', '3', '6.00'),
            ('1', 2, '3', '6.00'),
            ('1.5', 2, '3', '6.50'),
            ('1.50', 2, '3', '6.50'),
            ('-1.50', 2, '3', '3.50'),
            ('1.81', 2, '3', '6.81'),
        ],
    ),
    (
        "subNumeric(p::column0, p::column1, p::column2)",
        [
            (9, 3, 2, '4.00'),
            ('9', 3, 2, '4.00'),
            ('9.4', 3, 2, '4.40'),
            ('9.40', 3, '2', '4.40'),
            ('1.40', 3, '2', '-3.60'),
            ('-1.40', 3, '2', '-6.40'),
            ('-1.40', -3, '-2', '3.60'),
        ],
    ),
    (
        "multNumeric(p::column0, p::column1, p::column2)",
        [
            (1, 2, 3, '6.00'),
            (1, 0, 3, '0.00'),
            (2, 2, 3, '12.00'),
            (2, 2, -3, '-12.00'),
            ('2.1', -2, -3, '12.60'),
            ('2.10', -2, -3, '12.60'),
            (1000, '0.001', -3, '-3.00'),
        ],
    ),
    (
        "divNumeric(p::column0, p::column1)",
        [
            (6, 2, '3.00'),
            (7, 2, '3.50'),
            (2, 3, '0.67'),
            ('7.5', '-2', '-3.75'),
            ('-7.5', -2, '3.75'),
            ('-7.5', 0, '-Infinity'),
            ('-7.5', '-0', 'Infinity'),
            ('5.53', 0, 'Infinity'),
            (0, 0, 'NaN'),
            ('-0.00', 0, 'NaN'),
            ('-7.5', 1, '-7.50'),
            ('0', '1.1', '0.00'),
            (1, '0.000001', '1000000.00'),
            ('0.00211', '0.000001', '2110.00'),
            ('2.72', '-0.000001', '-2720000.00'),
        ],
    ),
])

# String tests
xdfTests.extend([
    (
        "concat(p::column0, p::column1, p::column2)",
        [
            ("a", "b", "c", "abc"),
        ],
    ),
    (
        "concat(p::column0, p::column1, p::column2, p::column3)",
        [
            ("a", ".Xc.", "b", "\n", "a.Xc.b\n"),
        ],
    ),
    (
        "soundEx(p::column0)",
        [
            ("Ashcraft", "A261"),
            ("Ashcroft", "A261"),
            ("Burroughs", "B620"),
            ("Burrows", "B620"),
            ("Ekzampul", "E251"),
            ("Example", "E251"),
            ("Ellery", "E460"),
            ("Euler", "E460"),
            ("Ghosh", "G200"),
            ("Gauss", "G200"),
            ("Gutierrez", "G362"),
            ("Heilbronn", "H416"),
            ("Hilbert", "H416"),
            ("Jackson", "J250"),
            ("Kant", "K530"),
            ("Knuth", "K530"),
            ("Lee", "L000"),
            ("Lukasiewicz", "L222"),
            ("Lissajous", "L222"),
            ("Ladd", "L300"),
            ("Lloyd", "L300"),
            ("Moses", "M220"),
            ("O'Hara", "O600"),
            ("Pfister", "P236"),
            ("Rubin", "R150"),
            ("Robert", "R163"),
            ("Rupert", "R163"),
            ("Rubin", "R150"),
            ("Soundex", "S532"),
            ("Sownteks", "S532"),
            ("Tymczak", "T522"),
            ("VanDeusen", "V532"),
            ("Washington", "W252"),
            ("Wheaton", "W350"),
            ("a", "A000"),
            ("ab", "A100"),
            ("abc", "A120"),
            ("abcd", "A123"),
        ],
    ),
    (
        "levenshtein(p::column0, p::column1)",
        [
            ("kitten", "kitten", 0),
            ("kitten", "itten", 1),
            ("kitten", "kittel", 1),
            ("kitten", "akitten", 1),
            ("kitten", "itte", 2),
            ("kitten", "", 6),
            ("kitten", "dog", 6),
            ("kitten", "kn", 4),
            ("kitten", "kittten", 1),
            ("", "", 0),
            ("a", "a", 0),
            ("a", "", 1),
            ("a", "b", 1),
        ],
    ),
    (
        "stringLPad(p::column0, p::column1, p::column2)",
        [
            ("abc", 5, "a", "aaabc"),
            ("abc", 5, "abc", "ababc"),
            ("abc", 5, " ", "  abc"),
            ("abc", 5, "#######", "##abc"),
            ("abc", 2, "#######", "ab"),
        ],
    ),
    (
        "stringRPad(p::column0, p::column1, p::column2)",
        [
            ("abc", 5, "a", "abcaa"),
            ("abc", 5, "abc", "abcab"),
            ("abc", 5, " ", "abc  "),
            ("abc", 5, "#######", "abc##"),
            ("abc", 2, "#######", "ab"),
        ],
    ),
    (
        "initCap(p::column0)",
        [
            ("a b c", "A B C"),
            ("a B c", "A B C"),
            ("1a b2 33", "1a B2 33"),
            ("AA1 BB2 CC3", "Aa1 Bb2 Cc3"),
            ("aA bB cC", "Aa Bb Cc"),
        ],
    ),
    (
        "stringReverse(p::column0)",
        [
            ("a b c", "c b a"),
            ("a", "a"),
            ("123/321", "123/321"),
        ],
    ),
    (
        "ascii(p::column0)",
        [
            ("a b c", 97),
            ("AbC", 65),
            (" ", 32),
            ("|", 124),
        ],
    ),
    (
        "chr(p::column0)",
        [
            (0, ""),
            (97, "a"),
            (65, "A"),
            (32, " "),
            (124, "|"),
        ],
    ),
    (
        "bitLength(p::column0)",
    # We use 8-bytes for fixed size types
    # strings take 8 bytes + strlen + 1
        [
            (1, 64),
            (1.1, 64),
            ("abc", 96),
            (True, 8),
        ],
    ),
    (
        "octetLength(p::column0)",
        [
            (1, 8),
            (1.1, 8),
            ("abc", 12),
            (True, 1),
        ],
    ),
    (
        "formatNumber(p::column0, p::column1)",
        [
            (1, 0, "1"),
            (1000, 0, "1,000"),
            (1000, 1, "1,000.0"),
            (1000.1, 2, "1,000.10"),
            (1000.5, 0, "1,000"),
            (20000000.5555, 3, "20,000,000.556"),
            (1.9999, 3, "2.000"),
        ],
    ),
    # string, search string, ignore case
    (
        "contains(p::column0, p::column1, p::column2)",
        [
            ("abcdef", "bcd", False, True),
            ("abcdef", "BcD", False, False),
            ("abcdef", "BcD", True, True),
            ("abcdef", "abcdefg", False, False),
            ("ABCDEF", "abcdefg", True, False),
            ("12#$%AbCd", "#$%A", False, True),
            ("12#$%AbCd", "#$%a", False, False),
            ("12#$%AbCd", "#$%a", True, True),
        ],
    ),
    # string, search string, ignore case
    (
        "startsWith(p::column0, p::column1, p::column2)",
        [
            ("abcdef", "abc", False, True),
            ("abcdef", "AbC", False, False),
            ("abcdef", "AbC", True, True),
            ("abcdef", "abcdefg", False, False),
            ("ABCDEF", "abcdefg", True, False),
            ("abcabc", "abc", False, True),
            ("abcabc", "AbC", False, False),
            ("abcabc", "AbC", True, True),
            ("abcabc", "abcdefg", False, False),
            ("ABCABC", "abcdefg", True, False),
        ],
    ),
    # string, search string, ignore case
    (
        "endsWith(p::column0, p::column1, p::column2)",
        [
            ("abcdef", "def", False, True),
            ("abcdef", "DeF", False, False),
            ("abcdef", "DeF", True, True),
            ("abcdef", "abcdefg", False, False),
            ("ABCDEF", "abcdefg", True, False),
            ("defdef", "def", False, True),
            ("defdef", "DeF", False, False),
            ("defdef", "DeF", True, True),
            ("defdef", "abcdefg", False, False),
            ("DEFDEF", "abcdefg", True, False),
        ],
    ),
    # original string, search string, replace string, ignore case
    (
        "replace(p::column0, p::column1, p::column2, p::column3)",
        [
            ("abcdef", "cde", "xxx", False, "abxxxf"),
            ("abcdef", "CDE", "xxx", False, "abcdef"),
            ("abcdef", "CDE", "xxx", True, "abxxxf"),
            ("abcdef", "CDE", "xxxx", True, "abxxxxf"),
            ("abcdef", "CDE", "x", True, "abxf"),
            ("12#$%AbCd", "#$%A", "xxxxxxx", False, "12xxxxxxxbCd"),
            ("12#$%AbCd", "#$%a", "xxxxxxx", True, "12xxxxxxxbCd"),
            ("12#$%AbCd", "#$%a", "xxxxxxx", False, "12#$%AbCd"),
        ],
    ),
    # Combine a bunch of random strings and make sure it's the same in XCE
    ("concat(p::column0, p::column1)",
     [(a, b, a + b) for a, b in ((randomString(50), randomString(50))
                                 for _ in range(numRecords))]),
    (
        "upper(p::column0)",
        [
            ("abcdef", "ABCDEF"),
            ("ABCDEF", "ABCDEF"),
            ("aBcDeF", "ABCDEF"),
            ("12345", "12345"),
            ("12#$%AbCd", "12#$%ABCD"),
        ],
    ),
    (
        "lower(p::column0)",
        [
            ("abcdef", "abcdef"),
            ("ABCDEF", "abcdef"),
            ("aBcDeF", "abcdef"),
            ("12345", "12345"),
            ("12#$%AbCd", "12#$%abcd"),
        ],
    ),
    (
        "repeat(p::column0, p::column1)",
        [
            ("abc", 0, ""),
            ("abc", 1, "abc"),
            ("abc", 2, "abcabc"),
            ("abc", 3, "abcabcabc"),
            ("abc", -2, ""),
            ("7", 0, ""),
            ("7", 1, "7"),
            ("7", 2, "77"),
            ("7", 3, "777"),
        ],
    ),
])

# If tests
xdfTests.extend([
    (
        "if(p::column0, p::column1, p::column2)",
        [
            (True, 1.0, 2.0, 1.0),
            (False, 1.0, 2.0, 2.0),
            (None, 1.0, 2.0, None),
        ],
    ),
    (
        "if(p::column0, p::column1, p::column2, p::column3)",
        [
            (True, 1.0, 2.0, False, 1.0),
            (True, 1.0, 2.0, True, 1.0),
            (False, 1.0, 2.0, False, 2.0),
            (False, 1.0, 2.0, True, 2.0),
            (True, 1.0, None, False, 1.0),
            (None, 1.0, 2.0, False, None),
            (None, 1.0, 2.0, True, 2.0),
        ],
    ),
    (
        "ifStr(p::column0, p::column1, p::column2)",
        [
            (True, "a", "b", "a"),
            (False, "a", "b", "b"),
            (None, "a", "b", None),
        ],
    ),
    (
        "ifStr(p::column0, p::column1, p::column2, p::column3)",
        [
            (True, "a", "b", False, "a"),
            (True, "a", "b", True, "a"),
            (False, "a", "b", False, "b"),
            (False, "a", "b", True, "b"),
            (True, "a", None, False, "a"),
            (None, "a", "b", False, None),
            (None, "a", "b", True, "b"),
        ],
    ),
    (
        "ifInt(p::column0, p::column1, p::column2)",
        [
            (True, 1, 2, 1),
            (False, 1, 2, 2),
            (None, 1, 2, None),
        ],
    ),
    (
        "ifInt(p::column0, p::column1, p::column2, p::column3)",
        [
            (True, 1, 2, False, 1),
            (True, 1, 2, True, 1),
            (False, 1, 2, False, 2),
            (False, 1, 2, True, 2),
            (True, 1, None, False, 1),
            (None, 1, 2, False, None),
            (None, 1, 2, True, 2),
        ],
    ),
    (
        "ifNumeric(p::column0, money(p::column1), money(p::column2))",
        [
            (True, '1.00', '2.00', '1.00'),
            (False, '1.00', '2.00', '2.00'),
            (None, '1.00', '2.00', None),
        ],
    ),
    (
        "ifNumeric(p::column0, money(p::column1), money(p::column2), p::column3)",
        [
            (True, '1.00', '2.00', False, '1.00'),
            (True, '1.00', '2.00', True, '1.00'),
            (False, '1.00', '2.00', False, '2.00'),
            (False, '1.00', '2.00', True, '2.00'),
            (True, '1.00', None, False, '1.00'),
            (None, '1.00', '2.00', False, None),
            (None, '1.00', '2.00', True, '2.00'),
        ],
    ),
    (
        "ifTimestamp(p::column0, timestamp(p::column1), timestamp(p::column2))",
        [
            (True, "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.002Z",
             "1970-01-01T00:00:00.001Z"),
            (False, "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.002Z",
             "1970-01-01T00:00:00.002Z"),
            (None, "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.002Z",
             None),
        ],
    ),
    (
        "ifTimestamp(p::column0, timestamp(p::column1), timestamp(p::column2), p::column3)",
        [
            (True, "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.002Z",
             False, "1970-01-01T00:00:00.001Z"),
            (True, "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.002Z",
             True, "1970-01-01T00:00:00.001Z"),
            (False, "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.002Z",
             False, "1970-01-01T00:00:00.002Z"),
            (False, "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.002Z",
             True, "1970-01-01T00:00:00.002Z"),
            (True, "1970-01-01T00:00:00.001Z", None, False,
             "1970-01-01T00:00:00.001Z"),
            (None, "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.002Z",
             False, None),
            (None, "1970-01-01T00:00:00.001Z", "1970-01-01T00:00:00.002Z",
             True, "1970-01-01T00:00:00.002Z"),
        ],
    ),
])

# Bitwise, logical, unary math tests
xdfTests.extend([
    (
        "bitor(p::column0,1)",
        [(inVal, inVal | 1) for inVal in range(numRecords)],
    ),
    (
        "bitand(p::column0,7)",
        [(inVal, inVal & 7) for inVal in range(numRecords)],
    ),
    (
        "bitxor(p::column0,255)",
        [(inVal, inVal ^ 255) for inVal in range(numRecords)],
    ),
    (
        "bitlshift(p::column0,4)",
        [(inVal, inVal << 4) for inVal in range(numRecords)],
    ),
    (
        "bitrshift(p::column0,4)",
        [(inVal, inVal >> 4) for inVal in range(numRecords)],
    ),
    (
        "colsDefinedBitmap(p::column0, p::column1, p::column2)",
        [("abcdef", 1, "xxx", 7), ("def", 2, "x", 7), (None, 2, "x", 3),
         (None, 2, None, 2), (None, None, None, 0)],
    ),
    ("bitCount(p::column0)", [(1, 1), (3, 2), (-1, 64), (None, None)]),
    (
        "if(float(bitor(p::column0,1)),1.5,0.1)",
        [
            (0, 1.5),
            (1, 1.5),
            (2, 1.5),
        ],
    ),
    (
        "if(bitand(p::column0,0),1,0)",
        [
            (0, 0),
            (1, 0),
            (2, 0),
        ],
    ),
    makeUnaryMathTest(math.sin),
    makeUnaryMathTest(math.asin, -1, 1),
    makeUnaryMathTest(math.sinh, -100, 100),    # don't overflow
    makeUnaryMathTest(math.asinh),
    makeUnaryMathTest(math.cos),
    makeUnaryMathTest(math.acos, -1, 1),
    makeUnaryMathTest(math.cosh, -100, 100),    # don't overflow
    makeUnaryMathTest(math.acosh, 0, 10**40),
    makeUnaryMathTest(math.tan),
    makeUnaryMathTest(math.atan),
    makeUnaryMathTest(math.tanh),
    makeUnaryMathTest(math.atanh, -1, 1),
    makeUnaryMathTest(math.log, 0, 10**40),
])


def verifyUniqueRows(outRows):
    return all([
        len(list(group)) == 1 for key, group in groupby(
            sorted(outRows, key=lambda r: r["mappedField"]))
    ], )


# Casting tests
xdfTests.extend([
    (
        "int(p::column0, p::column1)",
        [
            ("ABC", 10, None),
            ("123ABC", 10, 123),
            ("1.0.1", 10, 1),
            ("1.2.3", 10, 1),
            ("010", 10, 10),
            ("0x00FF", 10, 0),
        ],
    ),
    (
        "float(p::column0)",
        [
            ("ABC", None),
            ("123ABC", 123.0),
            ("1.0.1", 1.0),
            ("1.2.3", 1.2),
            ("010", 10.0),
            ("0x00FF", 255.0),
        ],
    ),
    ("genUnique()", [() for _ in range(numRecords)], verifyUniqueRows),
    (
        "ceil(p::column0)",    # Make sure arrays get ignored
        [
            ([1, 2, 3], None),
            (1.1, 2),
        ],
    ),
])

# Comparison tests
xdfTests.extend([
    (
        "lt(p::column0, p::column1)",
        [
            (1, 1, False),
            (1, 1.1, True),
            (1.2, 1.1, False),
        ],
    ),
    (
        "le(p::column0, p::column1)",
        [
            (1, 1, True),
            (1, 1.1, True),
            (1.2, 1.1, False),
        ],
    ),
    (
        "eq(p::column0, p::column1)",
        [
            (1, 1, True),
            (1, 1.0, True),
            (1.1, 1.2, False),
            (None, None, True),
            (None, 1, False),
        ],
    ),
    # this is true with default config, autoscale=True, scalesDigits=2
    (
        "eq(money(p::column0), money(p::column1))",
        [
            ("4.987464", 4.987464, True),
            (9.12002, 9.12001, True),
            ("9.12002", 9.12001, True),
            ("9.12002", "9.12001", True),
            ("9.12002", 9.11002, False),
            (9.12002, 9.11002, False),
        ],
    ),
    (
        "eqNonNull(p::column0, p::column1)",
        [
            (1, 1, True),
            (1, 1.0, True),
            (1.1, 1.2, False),
            (None, 1, None),
            (1, None, None),
            (None, None, None),
        ],
    ),
])

andRes = [(True, True, True), (True, False, False), (True, None, None),
          (False, True, False), (False, False, False), (False, None, False),
          (None, True, None), (None, False, False), (None, None, None)]

orRes = [(True, True, True), (True, False, True), (True, None, True),
         (False, True, True), (False, False, False), (False, None, None),
         (None, True, True), (None, False, None), (None, None, None)]

xdfTests.extend([
    ("and(p::column0, p::column1)", [[col1, col2, None]
                                     for col1 in [True, False, None]
                                     for col2 in [True, False, None]],
     lambda outRows: all([((row["p::column0"], row["p::column1"], row["mappedField"] if "mappedField" in row else None) in andRes) for row in outRows])
     ),
    ("or(p::column0, p::column1)", [[col1, col2, None]
                                    for col1 in [True, False, None]
                                    for col2 in [True, False, None]],
     lambda outRows: all([((row["p::column0"], row["p::column1"], row["mappedField"] if "mappedField" in row else None) in orRes) for row in outRows])
     ),
])

xdfTests.extend([(
    "float(p::column0)",
    [
        ('1.23', 1.23),
        ('inf', 'inf'),    # Test inf
        ('inff', None),
        ('inf ', None),
        (' inf', 'inf'),
        (' inf ', None),
        ('inf f', None),
        (' inf f', None),
        ('+inf', 'inf'),    # Test +inf
        ('+inff', None),
        ('+inf ', None),
        (' +inf', 'inf'),
        (' +inf ', None),
        ('+inf f', None),
        (' +inf f', None),
        ('-inf', '-inf'),    # Test -inf
        ('-inff', None),
        ('-inf ', None),
        (' -inf', '-inf'),
        (' -inf ', None),
        ('-inf f', None),
        (' -inf f', None),
        ('nan', 'nan'),    # Test nan
        ('nann', None),
        (' nan', 'nan'),
        ('nan ', None),
        (' nan ', None),
        ('nan n', None),
        (' nan n', None),
    ])])

xdfTests.extend([(
    "string(float(p::column0))",
    [
        ('1.23', "1.23"),
        ('inf', "inf"),    # Test inf
        (' inf', "inf"),
        ('inff', None),
        ('inf f', None),
        (' inf f', None),
        ('+inf', "inf"),    # Test +inf
        (' +inf', "inf"),
        ('+inff', None),
        ('+inf f', None),
        (' +inf f', None),
        ('-inf', "-inf"),    # Test -inf
        ('-inf', "-inf"),
        ('-inff', None),
        ('-inf f', None),
        (' -inf f', None),
        ('nan', "nan"),    # Test nan
        (' nan', "nan"),
        ('nann', None),
        ('nan n', None),
        (' nan n', None),
    ])])


def a(b, c):
    pass


xdfAggTests = [
    ("listAgg(p-column0)", [
        ("A", ),
        ("AA", ),
        ("AAA", ),
        ("AAAA", ),
    ], {
        "constant": "AAAAAAAAAA"
    }),
]

xdfNegativeTests = [
    ("eq(p::column0)", [
        (1, 1),
    ], StatusT.StatusAstWrongNumberOfArgs),
    ("neq(p::column0)", [
        (2, 3),
    ], StatusT.StatusAstWrongNumberOfArgs),
]

# Generate some random integers to use in more tests
intInputs = list(range(1001))


class TestXDFs(object):
    def setup_class(cls):
        cls.client = Client()
        cls.xcalarApi = XcalarApi()
        cls.session = cls.client.create_session(testXDFsSessionName)
        cls.xcalarApi.setSession(cls.session)
        cls.operators = Operators(cls.xcalarApi)

    def teardown_class(cls):
        cls.client.destroy_session(testXDFsSessionName)
        cls.xcalarApi.setSession(None)
        cls.session = None
        cls.xcalarApi = None

    @pytest.mark.parametrize("xdfSuite", xdfTests, ids=lambda x: x[0])
    def testXdfs(self, xdfSuite):
        evalStr = xdfSuite[0]
        cases = xdfSuite[1]
        verifyFunc = xdfSuite[2] if len(xdfSuite) > 2 else None
        tables = ["indexed-xdf-table", "mapped-xdf-table"]
        dsName = "testXdfGeneratedDs"
        mappedFieldName = "mappedField"

        jsonCases = []
        for case in cases:
            caseObj = {}
            for ii, arg in enumerate(case[:-1]):
                caseObj["column{}".format(ii)] = arg
            jsonCases.append(caseObj)

        with tempfile.NamedTemporaryFile('w', encoding="utf-8") as tmpf:
            tmpf.write(json.dumps(jsonCases))
            tmpf.flush()
            dataset = JsonDataset(self.xcalarApi, DefaultTargetName, tmpf.name,
                                  dsName)
            dataset.load()

        self.operators.indexDataset(dataset.name, tables[0], "xcalarRecordNum",
                                    "p")
        self.operators.map(tables[0], tables[1], [evalStr], [mappedFieldName])

        resultSet = ResultSet(self.client, table_name=tables[1], session_name=testXDFsSessionName)

        if verifyFunc:
            # default verify function
            assert verifyFunc(resultSet.record_iterator())
        else:
            expectedRows = [{
                "p::column{}".format(ii): val
                for ii, val in enumerate(case[:-1])
            } for case in cases]
            for ii, row in enumerate(expectedRows):
                # Note that this is somewhat hacky, since there may be XDFs which
                # output an actual 'null', but this does not exist today
                if cases[ii][-1] is not None:
                    row[mappedFieldName] = cases[ii][-1]

            datasetCheck(expectedRows, resultSet.record_iterator())

        for table in reversed(tables):
            self.operators.dropTable(table)

        dataset.delete()

    @pytest.mark.parametrize("xdfSuite", xdfAggTests, ids=lambda x: x[0])
    def testAggXdfs(self, xdfSuite):
        evalStr = xdfSuite[0]
        cases = xdfSuite[1]
        expectedVal = xdfSuite[2]
        tables = ["indexed-xdf-table", "mapped-xdf-table"]
        dsName = "testAggXdfGeneratedDs"
        aggName = "mappedField"

        jsonCases = []
        for case in cases:
            caseObj = {}
            for ii, arg in enumerate(case):
                caseObj["column{}".format(ii)] = arg
            jsonCases.append(caseObj)

        with tempfile.NamedTemporaryFile('w', encoding="utf-8") as tmpf:
            tmpf.write(json.dumps(jsonCases))
            tmpf.flush()
            dataset = JsonDataset(self.xcalarApi, DefaultTargetName, tmpf.name,
                                  dsName)
            dataset.load()

        self.operators.indexDataset(
            dataset.name,
            tables[0],
            "column0",
            "p",
            ordering=XcalarOrderingT.XcalarOrderingAscending)
        self.operators.aggregate(tables[0], tables[1], evalStr)

        resultSet = ResultSet(self.client, table_name=tables[1], session_name=testXDFsSessionName)

        assert expectedVal == next(resultSet.record_iterator())

        for table in reversed(tables):
            self.operators.dropTable(table)
        dataset.delete()

    @pytest.mark.parametrize(
        "xdfNegativeSuite", xdfNegativeTests, ids=lambda x: x[0])
    def testXdfsNegative(self, xdfNegativeSuite):
        evalStr = xdfNegativeSuite[0]
        cases = xdfNegativeSuite[1]
        errStatus = xdfNegativeSuite[2]
        tables = ["indexed-xdf-table", "mapped-xdf-table"]
        dsName = "testXdfGeneratedDs"
        mappedFieldName = "mappedField"

        jsonCases = []
        for case in cases:
            caseObj = {}
            for ii, arg in enumerate(case):
                caseObj["column{}".format(ii)] = arg
            jsonCases.append(caseObj)

        with tempfile.NamedTemporaryFile('w', encoding="utf-8") as tmpf:
            tmpf.write(json.dumps(jsonCases))
            tmpf.flush()
            dataset = JsonDataset(self.xcalarApi, DefaultTargetName, tmpf.name,
                                  dsName)
            dataset.load()

        self.operators.indexDataset(dataset.name, tables[0], "xcalarRecordNum",
                                    "p")
        with pytest.raises(XcalarApiException) as e:
            self.operators.map(tables[0], tables[1], [evalStr],
                               [mappedFieldName])
        assert (e.value.status == errStatus)

        self.operators.dropTable(tables[0])
        dataset.delete()
