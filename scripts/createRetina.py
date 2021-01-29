
# coding: utf-8

# In[26]:


#Xcalar imports. For more information, refer to discourse.xcalar.com
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Session import Session
from xcalar.external.LegacyApi.WorkItem import WorkItem
from xcalar.external.LegacyApi.Operators import *
from xcalar.external.LegacyApi.Dataset import *
from xcalar.external.LegacyApi.WorkItem import *
from xcalar.external.LegacyApi.Udf import *
from xcalar.external.Retina import *
import timeit
import argparse
import json
import tarfile
import io
from tarfile import TarInfo

#Code starts here. First create a XcalarApi object to do anything
xcalarApi = XcalarApi(bypass_proxy=True)
op = Operators(xcalarApi)
udf = Udf(xcalarApi)
retina = Retina(xcalarApi)

user = "admin"
session = "test"

#Connect to current workbook that you are in
workbook = Session(xcalarApi, "admin", "admin", 4135730, True, "test")
xcalarApi.setSession(workbook)

pathDelim = '\"/\"'
cutNum = 8

finalTable = "Transactions"
finalTableKey = "txnId"

tables = {
    "orders": {
        "key": "o_orderkey",
        "opcode": "opcode",
        "columns": [
            {
                "name": "o_custkey",
                "type": "DfInt64"
            },
            {
                "name": "o_orderstatus",
                "type": "DfString"
            },
            {
                "name": "o_totalprice",
                "type": "DfFloat64"
            },
            {
                "name": "o_orderdate",
                "type": "DfString"
            },
            {
                "name": "o_orderpriority",
                "type": "DfString"
            },
            {
                "name": "o_shippriority",
                "type": "DfInt64"
            },
            {
                "name": "o_clerk",
                "type": "DfString"
            },
            {
                "name": "o_comment",
                "type": "DfString"
            }
        ],
        "path": "/netstore/users/vgonela/hvr_data/tpch/orders/",
    }
}

def formatColumns(colsIn, key, prefix = None):
    cols = []

    for col in colsIn:
        synthCol = {}
        if prefix:
            synthCol["sourceColumn"] = prefix + "::" + col["name"]
        else:
            synthCol["sourceColumn"] = col["name"]

        synthCol["destColumn"] = col["name"]
        synthCol["columnType"] = col["type"]

        cols.append(synthCol)

    cols.append({"sourceColumn": key,
                 "columnType": "DfInt64",
                 "destColumn": key})

    cols.append({"sourceColumn": "XcalarRankOver",
                 "columnType": "DfInt64",
                 "destColumn": "XcalarRankOver"})

    cols.append({"sourceColumn": "XcalarOpCode",
                 "columnType": "DfInt64",
                 "destColumn": "XcalarOpCode"})

    return (cols)


for tableName, info in tables.items():
    query = []

    load = {
        "operation": "XcalarApiBulkLoad",
        "args": {
            "dest": ".XcalarDS.{}".format(tableName),
            "loadArgs": {
                "parseArgs": {
                    "parserFnName": "default:convertNewLineJsonToArrayJson",
                    "parserArgJson": "{}",
                    "fileNameFieldName": "fn",
                    "recordNumFieldName": "rec",
                    "allowFileErrors": False,
                    "allowRecordErrors": False,
                    "schema": []
                },
                "sourceArgsList": [
                    {
                        "recursive": False,
                        "path": info["path"],
                        "targetName": "Default Shared Root",
                        "fileNamePattern": ""
                    }
                ],
                "size": 10737418240
            }
        }
    }

    query.append(load)

    index = [
        {
            "operation": "XcalarApiIndex",
            "args": {
                "source": ".XcalarDS.{}".format(tableName),
                "dest": "{}-1".format(tableName),
                "prefix": tableName,
                "key": [
                    {
                        "name": "xcalarRecordNum",
                        "ordering": "Unordered",
                        "keyFieldName": "",
                        "type": "DfInt64"
                    }
                ],
            },
        },
        {
            "operation": "XcalarApiMap",
            "tag": "sortTimestamp",
            "args": {
                "source": "{}-1".format(tableName),
                "dest": "{}-map".format(tableName),
                "eval": [
                    {
                        "evalString": "int(cut({}::fn, {}, {}))".format(tableName,
                                                                        cutNum,
                                                                        pathDelim),
                        "newField": "ts"
                    },
                    {
                        "evalString": "int({}::rec, 10)".format(tableName),
                        "newField": "rec"
                    }
                ],
            },
        },
        {
            "operation": "XcalarApiIndex",
            "tag": "sortTimestamp",
            "comment": "apply ordering based on file row and timestamp",
            "args": {
                "source": "{}-map".format(tableName),
                "dest": "{}-sort".format(tableName),
                "key": [
                    {
                        "name": "ts",
                        "ordering": "Ascending",
                        "keyFieldName": "ts",
                        "type": "DfInt64"
                    },
                    {
                        "name": "rec",
                        "ordering": "Ascending",
                        "keyFieldName": "rec",
                        "type": "DfInt64"
                    }
                ],
            },
        }
    ]

    query += index

    rankOver = [
        {
            "operation": "XcalarApiGetRowNum",
            "tag": "rankOver",
            "args": {
                "source": "{}-sort".format(tableName),
                "dest": "{}-getRowNum".format(tableName),
                "newField": "rowNum"
            },
        },
        {
            "operation": "XcalarApiIndex",
            "tag": "rankOver",
            "args": {
                "source": "{}-getRowNum".format(tableName),
                "dest": "{}-keyIndex".format(tableName),
                "key": [
                    {
                        "name": "{}::{}".format(tableName, info["key"]),
                        "ordering": "Unordered",
                        "keyFieldName": info["key"],
                        "type": "DfInt64"
                    }
                ],
            },
        },
        {
            "operation": "XcalarApiGroupBy",
            "tag": "rankOver",
            "comment": "",
            "args": {
                "source": "{}-keyIndex".format(tableName),
                "dest": "{}-groupBy".format(tableName),
                "eval": [
                    {
                        "evalString": "minInteger(rowNum)",
                        "newField": "minRow"
                    }
                ],
                "newKeyField": "dummyKey",
            },
        },
        {
            "operation": "XcalarApiJoin",
            "tag": "rankOver",
            "args": {
                "source": [
                    "{}-keyIndex".format(tableName),
                    "{}-groupBy".format(tableName),
                ],
                "key": [
                    [
                        info["key"]
                    ],
                    [
                        "dummyKey"
                    ]
                ],
                "joinType": "innerJoin",
                "dest": "{}-joinBack".format(tableName),
                "columns": [
                    [
                        {
                            "sourceColumn": "orders",
                            "columnType": "DfFatptr",
                            "destColumn": "orders"
                        },
                        {
                            "sourceColumn": info["key"],
                            "columnType": "DfInt64",
                            "destColumn": info["key"],
                        }
                    ],
                    [
                        {
                            "sourceColumn": "dummyKey",
                            "columnType": "DfInt64",
                            "destColumn": "dummyKey"
                        },
                        {
                            "sourceColumn": "minRow",
                            "columnType": "DfInt64",
                            "destColumn": "minRow"
                        }
                    ]
                ],
            },
        },
        {
            "operation": "XcalarApiMap",
            "tag": "rankOver",
            "args": {
                "source": "{}-joinBack".format(tableName),
                "dest": "{}-ranked".format(tableName),
                "eval": [
                    {
                        "evalString": "int(add(sub(rowNum, minRow), 1))",
                        "newField": "XcalarRankOver"
                    },
                    {
                        "evalString": "int({}::{})".format(tableName,
                                                           info["opcode"]),
                        "newField": "XcalarOpCode"
                    }
                ],
            },
        }
    ]

    query += rankOver

    synthesizeColumns = formatColumns(info["columns"], info["key"])

    synth = {
        "operation": "XcalarApiSynthesize",
        "comment": "apply schema",
        "args": {
            "sameSession": True,
            "source": "{}-ranked".format(tableName),
            "dest": tableName,
            "columns": synthesizeColumns,
        },
    }

    query.append(synth)

    queryStr = json.dumps(query)

    print("Starting {}".format(tableName))
    print(queryStr)

    start = timeit.default_timer()
    xcalarApi.submitQuery(queryStr, session, tableName)

    try:
        op.unpublish(tableName)
    except:
        pass

    op.publish(tableName, tableName)
    end = timeit.default_timer()

    elapsed = end - start
    print("Ran {}: {}s".format(tableName, str(elapsed)))

    retinaColumns = [col["destColumn"] for col in synthesizeColumns]

    try:
        retina.delete(tableName)
    except:
        pass

    retina.make(tableName,
                [tableName],
                [retinaColumns])


batchId = 0

for tableName, info in tables.items():
    joinColumns = formatColumns(info["columns"], info["key"])

    dataflowInfo = {}

    dataflowInfo["query"] = [
        {
            "operation": "XcalarApiSelect",
            "comment": "Latest batch select on {}".format(tableName),
            "args": {
                "source": tableName,
                "dest": "{}.select".format(tableName),
                "minBatchId": batchId,
                "maxBatchId": batchId
            }
        },
        {
            "operation": "XcalarApiSelect",
            "comment": "Full historical select on {}".format(finalTable),
            "args": {
                "source": finalTable,
                "dest": "{}.select".format(finalTable)
            }
        },
        {
            "operation": "XcalarApiAggregate",
            "comment": "Find minimum key",
            "tag": "filterMinKey",
            "args": {
                "source": "{}.select".format(tableName),
                "dest": "{}.minKey".format(tableName),
                "eval": [
                    {
                        "evalString": "min({})".format(info["key"])
                    }
                ]
            }
        },
        {
            "operation": "XcalarApiFilter",
            "comment": "Reduce {} using calculated minKey".format(finalTable),
            "tag": "filterMinKey",
            "args": {
                "source": "{}.select".format(finalTable),
                "dest": "{}.filter".format(finalTable),
                "eval": [
                    {
                        "evalString": "gt({}, ^{}.minKey)".format(info["key"],
                                                                  tableName)
                    }
                ]
            },
        },
        {
            "operation": "XcalarApiJoin",
            "comment": "Apply changes to {} by doing a join".format(finalTable),
            "args": {
                "source": [
                    "{}.filter".format(finalTable),
                    "{}.select".format(tableName),
                ],
                "joinType": "crossJoin",
                "dest": "{}-joined".format(tableName),
                "evalString": "eq({}_, {})".format(info["key"], info["key"]),
                "columns": [
                    [
                        {
                            "sourceColumn": info["key"],
                            "columnType": "DfInt64",
                            "destColumn": info["key"] + "_"
                        },
                        {
                            "sourceColumn": finalTableKey,
                            "columnType": "DfInt64",
                            "destColumn": finalTableKey,
                        }
                    ],
                    joinColumns
                ],
            },
        }
    ]

    tableColumns = [{"columnName": col["destColumn"], "headerAlias": col["destColumn"]}
                    for col in joinColumns]

    dataflowInfo["tables"] = [
        {
            "name": "{}-joined".format(tableName),
            "columns": tableColumns
        }
    ]

    dataflowStr = json.dumps(dataflowInfo)

    retinaBuf = io.BytesIO()

    with tarfile.open(fileobj = retinaBuf, mode = "w:gz") as tar:
        info = TarInfo("dataflowInfo.json")
        info.size = len(dataflowStr)

        tar.addfile(info, io.BytesIO(bytearray(dataflowStr, "utf-8")))

    retina.add(tableName + "_applyUpdate", retinaBuf.getvalue())
