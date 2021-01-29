import tempfile
import time
import uuid

import pytest

from xcalar.compute.util.Qa import buildNativePath, DefaultTargetName
from xcalar.compute.util.Qa import datasetCheck

from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException

from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiColumnT


def makeBoolean(arg):
    return arg == "True"


typeMappings = {
    "DfString": str,
    "DfInt32": int,
    "DfInt64": int,
    "DfUInt32": int,
    "DfUInt64": int,
    "DfFloat32": float,
    "DfFloat64": float,
    "DfBoolean": makeBoolean
}

# Test cases which try loading these datasets with all types of schema
# specification
csvCases = [
    {
        "testname":
            "Easy sanity test",
        "parseArgs": {
            "recordDelim": '\n',
            "fieldDelim": ','
        },
        "typedColumns": [
            ("one", "DfString"),
            ("two", "DfString"),
            ("three", "DfString"),
            ("intHeader", "DfInt64"),
            ("floatHeader", "DfFloat64"),
            ("boolHeader", "DfBoolean"),
        ],
        "rawText":
            """\
first,second,third,1,2.2,True
fourth,fifth,sixth,3,4.1,False
""",
        "expectedRows": [["first", "second", "third", "1", "2.2", "True"],
                         ["fourth", "fifth", "sixth", "3", "4.1", "False"]],
    },
    # This test case is for SQL-243. But commenting out for now
    # as the validation looks for float value whereas resultset
    # will return inf as a string. This is due to our use of json
    # for resultsets and json cannot represent float values like
    # inf, nan. This test can be enabled once we move to use
    # protobuf for resultsets
    # {
    #     "testname": "load inf",
    #     "parseArgs": {
    #         "recordDelim": '\n',
    #         "fieldDelim": ','
    #     } ,
    #     "typedColumns": [("infHeader", "DfFloat64")],
    #     "rawText": "inf",
    #     "expectedRows": [["inf"]],
    # },
    {
        "testname":
            "escape hell",
        "parseArgs": {
            "recordDelim": '\n',
            "fieldDelim": ','
        },
        "typedColumns": [("one", "DfString"), ("two", "DfString"),
                         ("three", "DfString"), ("four", "DfString"),
                         ("five", "DfString")],
        "rawText":
            'this\\","should",be,five\\\\,"\\\\columns\\\\"',
        "expectedRows": [[
            'this\\"', 'should', 'be', 'five\\\\', '\\\\columns\\\\'
        ]],
    },
    {
        "testname":
            "multi char field delim hell",
        "parseArgs": {
            "recordDelim": '\n',
            "fieldDelim": 'abababc'
        },
        "typedColumns": [
            ("one", "DfString"),
            ("two", "DfString"),
            ("three", "DfString"),
        ],
        "rawText":
            "ababababccababababababcabab",
        "expectedRows": [['ab', 'cababab', 'abab']],
    },
    {
        "testname":
            "single quote test",
        "parseArgs": {
            "recordDelim": '\n',
            "fieldDelim": ',',
            "quoteDelim": "'"
        },
        "typedColumns": [
            ("one", "DfString"),
            ("two", "DfString"),
            ("three", "DfString"),
        ],
        "rawText":
            """\
'hello ,',"should be",correct
unescaped space,'single,
quote',"double quote"
""",
        "expectedRows": [
            ["hello ,", '"should be"', 'correct'],
            ['unescaped space', "single,\nquote", '"double quote"'],
        ],
    },
    {
        "testname":
            "no final record delimiter; single record",
        "parseArgs": {
            "recordDelim": '\n',
            "fieldDelim": ','
        },
        "typedColumns": [("one", "DfString"), ("two", "DfString"),
                         ("three", "DfString")],
        "rawText":
            "this should still load, second column, the last col",
        "expectedRows": [[
            'this should still load', ' second column', ' the last col'
        ]]
    },
    {
        "testname": "no final record delimiter; single col",
        "parseArgs": {
            "recordDelim": '\n',
            "fieldDelim": ','
        },
        "typedColumns": [("one", "DfString")],
        "rawText": "1\n2",
        "expectedRows": [
            ['1'],
            ['2'],
        ]
    },
    {
        "testname":
            "no final record delimiter; zero char field",
        "parseArgs": {
            "recordDelim": '\n',
            "fieldDelim": ','
        },
        "typedColumns": [
            ("one", "DfString"),
            ("two", "DfString"),
            ("three", "DfString"),
        ],
        "rawText":
            "1,2,",
        "expectedRows": [['1', '2', '']]
    },
    {
        "testname":
            "zero length fields",
        "parseArgs": {
            "recordDelim": '\n',
            "fieldDelim": ','
        },
        "typedColumns": [
            ("", "DfString"),
            ("", "DfString"),
            ("", "DfString"),
            ("", "DfString"),
        ],
        "rawText":
            """\
,,,
""",
        "expectedRows": [['', '', '', '']]
    },
    {
        "testname": "no record delimiter",
        "parseArgs": {
            "recordDelim": '',
            "fieldDelim": ','
        },
        "typedColumns": [],
        "rawText": """\
hello
world\
""",
        "expectedRows": [['hello\nworld']]
    },
    {
        "testname": "no field delim; long file",
        "parseArgs": {
            "recordDelim": '',
            "fieldDelim": ''
        },
        "typedColumns": [],
        "rawText": "hello" * 10000,
        "expectedRows": [['hello' * 10000]]
    },
    {
        "testname": "inconsistent number of columns",
        "parseArgs": {
            "recordDelim": '\n',
            "fieldDelim": ','
        },
        "typedColumns": [("first", "DfString"), ("second", "DfString")],
        "rawText": """\
one,two
1,2,3
""",
        "expectedRows": [[u'one', u'two'], [u'1', u'2', u'3']],
    },
    {
        "testname": "multi character record delimiter",
        "parseArgs": {
            "recordDelim": '||\r\n',
            "fieldDelim": ','
        },
        "typedColumns": [("", "DfString")],
        "rawText": """\
first||\r
second||\r
thirdp1
fourth
""",
        "expectedRows": [['first'], ['second'], ['thirdp1\nfourth\n']],
    },
    {
        "testname": "CRLF flag",
        "parseArgs": {
            "recordDelim": '\n',
            "fieldDelim": ',',
            'isCRLF': True
        },
        "typedColumns": [("", "DfString")],
        "rawText": """\
first\r
second\r
thirdp1
fourth
""",
        "expectedRows": [['first'], ['second'], ['thirdp1\nfourth\n']],
    },
    {
        "testname":
            "long file; multi character field, record, quote delimiter",
        "parseArgs": {
            "recordDelim": '||\r\n',
            "fieldDelim": ' | ',
            "quoteDelim": '""'
        },
        "typedColumns": [("", "DfString"), ("", "DfString"), ("", "DfString")],
        "rawText":
            """\
first1 | first2 | first3||\r
second1 | ""quoted | second2"" | second3||\r
thirdp1
thirdp2| | fourthp3||\r
""" * 10000,
        "expectedRows": [['first1', 'first2', 'first3'],
                         ['second1', 'quoted | second2', 'second3'],
                         ['thirdp1\nthirdp2|', 'fourthp3']] * 10000,
    },
    {
        "testname": "longest allowed field delimiter",
        "parseArgs": {
            "fieldDelim": 'a' * 254
        },
        "typedColumns": [("", "DfString")],
        "rawText": "f1" + ("a" * 254) + "f2" + ("a" * 254) + "f3",
        "expectedRows": [['f1', "f2", "f3"], ],
    },
    {
        "testname": "field delimiter too long",
        "parseArgs": {
            "fieldDelim": 'a' * 256
        },
        "typedColumns": [("", "DfString")],
        "errorString": "Failed to initialize parser",
    },
    {
        "testname": "longest allowed record delimiter",
        "parseArgs": {
            "recordDelim": "a" * 127
        },
        "typedColumns": [("", "DfString")],
        "rawText": "r1{0}r2{0}r3".format("a" * 127),
        "expectedRows": [['r1'], ['r2'], ['r3']],
    },
    {
        "testname": "record delimiter too long",
        "parseArgs": {
            "recordDelim": "a" * 128
        },
        "typedColumns": [("", "DfString")],
        "errorString": "Failed to initialize parser",
    },
    {
        "testname": "longest allowed quote delimiter",
        "parseArgs": {
            "quoteDelim": "'" * 127,
            "fieldDelim": ','
        },
        "typedColumns": [("", "DfString")],
        "rawText": "f1,{0}f2-1,f2-2{0},f3".format("'" * 127),
        "expectedRows": [['f1', 'f2-1,f2-2', 'f3'], ],
    },
    {
        "testname": "quote delimiter too long",
        "parseArgs": {
            "quoteDelim": "'" * 128
        },
        "typedColumns": [("", "DfString")],
        "errorString": "Failed to initialize parser",
    },
    {
        "testname": "escape within quotes",
        "parseArgs": {
            "fieldDelim": ""
        },
        "typedColumns": [("", "DfString")],
        "rawText": r'"random \" quote"',
        "expectedRows": [[r'random \" quote']]
    },
    {
        "testname":
            "escape within quotes customer10",
        "parseArgs": {
            "fieldDelim": ""
        },
        "typedColumns": [("column0", "DfString")],
        "rawText":
            r'''column0
67485,107073,1,Carlsbad,West,1900 Aston Avenue,NULL,92008,Carlsbad,NULL,CA,US,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:56.000
23593,103070,1,ZIMMER WARSAW MS 5106,ZIMALOY HIP STEM CELL,PO BOX 708,345 EAST MAIN STREET,46581-0708,WARSAW,NULL,IN,NULL,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:49.000
40909,104061,1,AUSTIN BIOLOGICS,SBI-R&D AUSTIN,12024 VISTA PARK DR,NULL,78726-0000,AUSTIN,NULL,TX,US,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:51.000
"40417,104061,1,\"AUSTIN, TX\",KNEE POROUS COATING,9900 SPECTRUM DR,NULL,78717-0000,AUSTIN,NULL,TX,US,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:51.000"
53914,107103,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:55.000
34230,108200,1,NULL,NULL,NULL,NULL,78762,AUSTIN,NULL,TX,US,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:54.000
"77680,109113,1,AustinSpine,Ops Distribution Svc,\"5301 Riata Park Court, Bldg F\",NULL,78727,Austin,NULL,TX,US,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:58.000"
467312,116062,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:14:06.000
''',
        "expectedRows": [
            [r'column0'],
            [
                r'67485,107073,1,Carlsbad,West,1900 Aston Avenue,NULL,92008,Carlsbad,NULL,CA,US,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:56.000'
            ],
            [
                r'23593,103070,1,ZIMMER WARSAW MS 5106,ZIMALOY HIP STEM CELL,PO BOX 708,345 EAST MAIN STREET,46581-0708,WARSAW,NULL,IN,NULL,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:49.000'
            ],
            [
                r'40909,104061,1,AUSTIN BIOLOGICS,SBI-R&D AUSTIN,12024 VISTA PARK DR,NULL,78726-0000,AUSTIN,NULL,TX,US,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:51.000'
            ],
            [
                r'40417,104061,1,\"AUSTIN, TX\",KNEE POROUS COATING,9900 SPECTRUM DR,NULL,78717-0000,AUSTIN,NULL,TX,US,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:51.000'
            ],
            [
                r'53914,107103,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:55.000'
            ],
            [
                r'34230,108200,1,NULL,NULL,NULL,NULL,78762,AUSTIN,NULL,TX,US,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:54.000'
            ],
            [
                r'77680,109113,1,AustinSpine,Ops Distribution Svc,\"5301 Riata Park Court, Bldg F\",NULL,78727,Austin,NULL,TX,US,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:13:58.000'
            ],
            [
                r'467312,116062,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,SOURCE\201805\F0116_180509.TXT,2018-05-10 14:14:06.000'
            ]
        ]
    },
]


def _loadTestDataset(workbook, fileContents, parserParams):
    dsName = "pytest-csv-{}".format(uuid.uuid4())

    with tempfile.NamedTemporaryFile('w', encoding="utf-8") as tmpf:
        tmpf.write(fileContents)
        tmpf.flush()

        dataset_builder = workbook.build_dataset(dsName, DefaultTargetName,
                                                 tmpf.name, "csv")

        for param in parserParams:
            dataset_builder.option(param, parserParams[param])

        dataset = dataset_builder.load()
    return dataset


@pytest.mark.parametrize(
    "schemaMode",
    ["none", "header", "loadInput", "loadInputWithHeader", "schemaFile"])
@pytest.mark.parametrize(
    "testCase", csvCases, ids=lambda case: case["testname"].replace(' ', '_'))
def test_csv_parser(workbook, schemaMode, testCase):
    parserParams = testCase["parseArgs"]
    typedColumns = testCase["typedColumns"]
    useHeader = schemaMode in ["header", "loadInputWithHeader"]
    if schemaMode == "loadInputWithHeader":
        parserParams["linesToSkip"] = 1
    # loadInputWithHeader uses normal loadInput, but puts a header in and tells
    # the parser to skip over it. This is what XD usually does as of 10/30/2018
    if schemaMode == "loadInputWithHeader":
        schemaMode = "loadInput"
    parserParams["schemaMode"] = schemaMode
    rawText = testCase.get("rawText", "")
    expectedRowsArray = testCase.get("expectedRows", [])
    expectedErrorMsg = testCase.get("errorString", None)
    shouldSucceed = not expectedErrorMsg

    testId = hash((frozenset(parserParams), rawText, schemaMode))
    schema = []

    recordDelim = parserParams.get("recordDelim", "\n")
    if parserParams.get("isCRLF", False):
        recordDelim = "\r\n"
    fieldDelim = parserParams.get("fieldDelim", "\t")

    if shouldSucceed:
        # Our test should either specify the expected rows or expected error
        assert len(expectedRowsArray)

    if schemaMode == "none":
        headers = []
    else:
        headers = [colName for (colName, colType) in typedColumns]
        if (schemaMode == "loadInput"):
            schema = [
                XcalarApiColumnT(colName, colName, colType)
                for (colName, colType) in typedColumns
            ]

    if schemaMode == "schemaFile":
        pytest.xfail("Not implemented yet")

    if useHeader and recordDelim == "":
        pytest.xfail("No way to get header if there's only 1 record")

    expectedRows = []
    for row in expectedRowsArray:
        expectedRow = {}
        for ii, fieldValue in enumerate(row):
            if ii >= len(headers) or len(headers[ii]) == 0:
                expectedRow["column%d" % ii] = fieldValue
            else:
                fieldName = headers[ii]
                if schemaMode == "loadInput":
                    convFunc = typeMappings[typedColumns[ii][1]]
                    expectedRow[fieldName] = convFunc(row[ii])
                else:
                    expectedRow[fieldName] = fieldValue
        expectedRows.append(expectedRow)

    fileContents = ""
    if (useHeader and len(headers) > 0):
        headerLine = fieldDelim.join(headers) + recordDelim
        fileContents += headerLine
    fileContents += rawText

    parserParams["schema"] = schema

    if shouldSucceed:
        dataset = _loadTestDataset(workbook, fileContents, parserParams)
        datasetCheck(expectedRows, dataset.records(), matchOrdering=True)
        dataset.delete()
    else:
        with pytest.raises(XcalarApiStatusException) as exc:
            dataset = _loadTestDataset(workbook, fileContents, parserParams)
        assert expectedErrorMsg in exc.value.output.loadOutput.errorString


def test_too_many_fields(workbook):
    parseArgs = {"recordDelim": "\n", "fieldDelim": ",", "schemaMode": "none"}
    errorString = "record: 2, max num fields exceeded"

    # Make a dataset with a row that has too many columns
    max_cols = 4096
    longrow = "," * max_cols    # note that this is max_cols + 1 columns
    rawText = "firstrow\nsecondrow\n{longrow}".format(longrow=longrow)

    with pytest.raises(XcalarApiStatusException) as exc:
        dataset = _loadTestDataset(workbook, rawText, parseArgs)
    assert errorString in exc.value.output.loadOutput.errorString


def test_skip_lines(workbook):
    path = buildNativePath("csvSanity/extendedHeader.csv")
    dataset_builder = workbook.build_dataset("skippedLines", DefaultTargetName,
                                             path, "csv")
    dataset_builder.option("fieldDelim", ',').option("linesToSkip", 5).option(
        "schemaMode", "header")
    dataset = dataset_builder.load()
    expectedRows = [{
        'myColumn': 'row0col0',
        'secondCol': 'row0col1'
    }, {
        'myColumn': 'row1col0',
        'secondCol': 'row1col1'
    }]
    datasetCheck(expectedRows, dataset.records())
    dataset.delete()


def test_skip_too_many_lines(workbook):
    path = buildNativePath("csvSanity/extendedHeader.csv")
    db = workbook.build_dataset("skippedTooManyLines", DefaultTargetName, path,
                                "csv")
    db.option("fieldDelim", ',').option("linesToSkip", 5000).option(
        "schemaMode", "header")
    dataset = db.load()
    assert dataset.record_count() == 0
    dataset.delete()


def test_invalid_field_name(workbook):
    path = buildNativePath("csvSanity/invalidColumnName.csv")
    db = workbook.build_dataset("invalidCol", DefaultTargetName, path, "csv")
    db.option("fieldDelim", ',').option("schemaMode", "header")
    with pytest.raises(XcalarApiStatusException) as exc:
        db.load()
    assert "invalidColumnName.csv" in exc.value.output.loadOutput.errorFile
    assert "Field header 'hasParen('" in exc.value.output.loadOutput.errorString


def test_empty_field_fnf_option(workbook):
    rawText = 'col0,col1,col2\n"val0",,""'
    expectedRows = [{"col0": "val0", "col2": ""}]
    parserArgs = {
        "schemaMode": "header",
        "fieldDelim": ",",
        "emptyAsFnf": True
    }
    with tempfile.NamedTemporaryFile('w', encoding="utf-8") as tmpf:
        tmpf.write(rawText)
        tmpf.flush()

        db = workbook.build_dataset("emptyAsFnfds", DefaultTargetName,
                                    tmpf.name, "csv")
        for arg in parserArgs:
            db.option(arg, parserArgs[arg])
        dataset = db.load()

    datasetCheck(expectedRows, dataset.records())
    dataset.delete()


# Returns the time taken
def _runBenchTest(workbook, numBytes, numFields, fieldSize):
    recordTemplate = ','.join(["a" * fieldSize for _ in range(numFields)])
    batchSize = 100 * 2**10 // len(recordTemplate)
    recBatch = "\n".join([recordTemplate for _ in range(batchSize)]) + "\n"
    numRecords = numBytes // len(recordTemplate)
    numTries = 1

    numBatches = numRecords // batchSize
    numLeftovers = numRecords - numBatches * batchSize

    durations = []
    with tempfile.NamedTemporaryFile(
            'w', buffering=2**20, encoding="utf-8") as tmpf:
        for _ in range(numBatches):
            tmpf.write(recBatch)
        for _ in range(numLeftovers):
            tmpf.write(recordTemplate + "\n")
        tmpf.flush()

        for _ in range(numTries):
            db = workbook.build_dataset("longFileDs", DefaultTargetName,
                                        tmpf.name, "csv")
            db.option("fieldDelim", ',').option("schemaMode", "none")
            start = time.time()
            dataset = db.load()
            duration = time.time() - start
            durations.append(duration)
            dataset.delete()
    avgDuration = sum(durations) / len(durations)
    return (numBytes / avgDuration) / 2**20


@pytest.mark.skip(reason="this throws an exception in order to show stdout")
def test_benchmark_csv(workbook):
    numBytes = 200 * 2**20
    numFieldOpts = [1, 3, 9, 27, 81]
    fieldSizeOpts = [1, 4, 16, 64, 256]

    print("MB/s," + ",".join(
        ["{} byte fields".format(fieldSize) for fieldSize in fieldSizeOpts]))

    for numFields in numFieldOpts:
        numFieldResults = []
        for fieldSize in fieldSizeOpts:
            duration = _runBenchTest(workbook, numBytes, numFields, fieldSize)
            numFieldResults.append(duration)
        fieldResults = ",".join([str(r) for r in numFieldResults])
        print("{} fields,{}".format(numFields, fieldResults))

    raise
