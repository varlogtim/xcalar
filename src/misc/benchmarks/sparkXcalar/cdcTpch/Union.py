
# coding: utf-8

# In[29]:


# Xcalar Notebook Connector
# 
# Connects this Jupyter Notebook to the Xcalar Workbook <untitled-xcalaruser>
#
# To use any data from your Xcalar Workbook, run this snippet before other 
# Xcalar Snippets in your workbook. 
# 
# A best practice is not to edit this cell.
#
# If you wish to use this Jupyter Notebook with a different Xcalar Workbook 
# delete this cell and click CODE SNIPPETS --> Connect to Xcalar Workbook.

get_ipython().run_line_magic('matplotlib', 'inline')

# Importing third-party modules to faciliate data work. 
import pandas as pd
import matplotlib.pyplot as plt

# Importing Xcalar packages and modules. 
# For more information, search and post questions on discourse.xcalar.com
from xcalar.compute.api.XcalarApi import XcalarApi
from xcalar.compute.api.Session import Session
from xcalar.compute.api.WorkItem import WorkItem
from xcalar.compute.api.ResultSet import ResultSet
from xcalar.compute.api.Operators import *
from xcalar.compute.api.Dataset import *
from xcalar.compute.api.WorkItem import *
from xcalar.compute.api.Udf import *
import timeit

# Create a XcalarApi object
xcalarApi = XcalarApi()
op = Operators(xcalarApi)
udf = Udf(xcalarApi)

# Connect to current workbook that you are in
workbook = Session(xcalarApi, "xcalaruser", "xcalaruser", 4654805, True, "untitled-xcalaruser")
xcalarApi.setSession(workbook)


# In[30]:


udfName = "udf"
udfSource = """
def sim(a, b, c):
    return str(c)
"""
udfEval = "udf:sim(L_QUANTITY, avg_qty, L_ORDERKEY)"

targetName = "azblob"

xcalarApi.setSession(workbook)
udf.addOrUpdate(udfName, udfSource)

load_start = timeit.default_timer()
tmpTable = "tmp"
key1Eval = "concat(concat(l_update::L_ORDERKEY, \",\"), l_update::L_LINENUMBER)"
key2Eval = "concat(concat(concat(l_update::L_ORDERKEY, \",\"), concat(l_update::L_LINENUMBER, \",\")), l_update::L_TIMESTAMP)"

# initialize names
lineUrl = "/tpch-sf100/lineitem/"
lineDs = "lineitem"
lineStart = "lineitem.start"
lineIndex = "lineitem.index"

numUpdates = 4
lineUpdateUrl = "/tpch-sf100-change/"
lineUpdateDs = []
lineUpdateStart = []
lineUpdateIndex = []
lineUpdateUnion = []
lineUpdateGroup = []
lineUpdateFinal = []
lineUpdateMap = []
lineUpdateFilter = []

op.dropTable("*")

for i in range(numUpdates):
    lineUpdateDs.append("update" + str(i) + ".ds")
    lineUpdateStart.append("update" + str(i) + ".start")
    lineUpdateIndex.append("update" + str(i) + ".index")
    lineUpdateUnion.append("update" + str(i) + ".union")
    lineUpdateGroup.append("update" + str(i) + ".group")
    lineUpdateFinal.append("update" + str(i) + ".final")
    lineUpdateMap.append("update" + str(i) + ".map")
    lineUpdateFilter.append("update" + str(i) + ".agg")

# load
print("Loading lineitem table")
try:
    dataset = CsvDataset(xcalarApi, targetName, lineUrl, lineDs,
                         fieldDelim = '|',
                         # 1 PB
                         sampleSize = 1024 * 1024 * 1024 * 1024 * 1024)
    dataset.load()
except:
    pass

for i in range(numUpdates):
    try:
        url = lineUpdateUrl + str(i + 1) + "/"

        dataset = CsvDataset(xcalarApi, targetName, url, lineUpdateDs[i],
                             fieldDelim = '|',
                             # 1 PB
                             sampleSize = 1024 * 1024 * 1024 * 1024 * 1024)
        dataset.load()
    except:
        pass


# In[31]:


# create starting tables
print("Creating tables")
try:
    op.indexDataset(".XcalarDS." + lineDs, tmpTable,
                    ["xcalarRecordNum"],
                    fatptrPrefixName = "l")
    op.map(tmpTable, lineStart,
           ["int(l::L_ORDERKEY)",
            "int(l::L_LINENUMBER)",
            "int(l::L_QUANTITY)"],
           ["L_ORDERKEY",
            "L_LINENUMBER",
            "L_QUANTITY"])
    op.dropTable(tmpTable)

    for i in range(numUpdates):
        op.indexDataset(".XcalarDS." + lineUpdateDs[i], tmpTable,
                        ["xcalarRecordNum"],
                        fatptrPrefixName = "l_update")

        op.map(tmpTable, lineUpdateStart[i],
               ["int(l_update::L_ORDERKEY)",
                "int(l_update::L_LINENUMBER)",
                "int(l_update::L_QUANTITY)",
                "int(l_update::L_TIMESTAMP)",
                key1Eval,
                key2Eval],
               ["L_ORDERKEY_UPDATE",
                "L_LINENUMBER_UPDATE",
                "L_QUANTITY_UPDATE",
                "L_TIMESTAMP_UPDATE",
                "key1",
                "key2"])
        op.dropTable(tmpTable)
except:
    pass

index_start = timeit.default_timer()
load_time = index_start - load_start


# In[32]:


# index
print("Indexing tables")
try:
    op.indexTable(lineStart, lineIndex,
                  ["L_ORDERKEY",
                   "L_LINENUMBER"])
except:
    pass

for i in range(numUpdates):
    tables = []
    for j in range(7):
        tables.append(str(j) + "tmp")
    
    op.indexTable(lineUpdateStart[i], tables[0], "key1")
    op.groupBy(tables[0], tables[1],
               ["maxInteger(L_TIMESTAMP_UPDATE)"], ["maxts"])
    op.map(tables[1], tables[2],
           ["concat(concat(key1, \",\"),string(maxts))"],
           ["key2_"])

    op.indexTable(tables[2], tables[3], "key2_")
    op.indexTable(lineUpdateStart[i], tables[4], "key2")

    op.join(tables[3], tables[4], tables[5],
            leftColumns =
            [XcalarApiColumnT("key1", "key1_tmp", "DfString")])
    op.project(tables[5], tables[6],
               ["L_ORDERKEY_UPDATE",
                "L_LINENUMBER_UPDATE",
                "L_QUANTITY_UPDATE",
                "l_update::L_COMMENT"])
    op.indexTable(tables[6], lineUpdateIndex[i],
                  ["L_ORDERKEY_UPDATE",
                   "L_LINENUMBER_UPDATE"])

    for table in tables:
        op.dropTable(table)

output = op.tableMeta(lineIndex)
srcRows = sum([ output.metas[ii].numRows for ii in range(output.numMetas)])
print("Lineitem table has " + str(srcRows) + " rows")

for i in range(numUpdates):
    output = op.tableMeta(lineUpdateIndex[i])
    srcRows = sum([ output.metas[ii].numRows for ii in range(output.numMetas)])
    print("Update table " + str(i) + " has " + str(srcRows) + " rows")

union_start = timeit.default_timer()
index_time = union_start - index_start


# In[33]:


# union
columns = [
    [
        XcalarApiColumnT("L_ORDERKEY_UPDATE", "L_ORDERKEY", "DfInt64"),
        XcalarApiColumnT("L_LINENUMBER_UPDATE", "L_LINENUMBER", "DfInt64"),
        XcalarApiColumnT("L_QUANTITY_UPDATE", "L_QUANTITY", "DfInt64"),
        XcalarApiColumnT("l_update", "l", "DfFatptr")
    ],
    [
        XcalarApiColumnT("L_ORDERKEY", "L_ORDERKEY", "DfInt64"),
        XcalarApiColumnT("L_LINENUMBER", "L_LINENUMBER", "DfInt64"),
        XcalarApiColumnT("L_QUANTITY", "L_QUANTITY", "DfInt64"),
        XcalarApiColumnT("l", "l", "DfFatptr")
    ],
]

unionTable = lineIndex

for i in range(numUpdates):
    newTable = "union" + str(i)
    day_start = timeit.default_timer()

    try:
        op.dropTable(newTable)
    except:
        pass

    op.union([lineUpdateIndex[i], unionTable], newTable, columns, dedup = True)
    unionTable = newTable

    day_union = timeit.default_timer()
    print("day {} union time {}".format(str(i),
                                        str(day_union - day_start)))

    op.indexTable(unionTable, lineUpdateUnion[i], "L_ORDERKEY")
    op.groupBy(lineUpdateUnion[i], lineUpdateGroup[i],
               "avg(L_QUANTITY)", "avg_qty")
    op.join(lineUpdateUnion[i], lineUpdateGroup[i], lineUpdateFinal[i],
            rightColumns =
            [XcalarApiColumnT("L_ORDERKEY", "gbkey_tmp", "DfInt64")])

    day_join = timeit.default_timer()
    print("day {} join-back time {}".format(str(i),
                                            str(day_join - day_union)))

    op.map(lineUpdateFinal[i], lineUpdateMap[i], udfEval, "udfCol")
    day_map = timeit.default_timer()

    print("day {} map time {}".format(str(i),
                                      str(day_map - day_start)))

    op.filter(lineUpdateMap[i], lineUpdateFilter[i], "eq(udfCol, \"1\")")

    print("day {} total time {}\n".format(str(i),
                                        str(timeit.default_timer() - day_start)))


union_time = timeit.default_timer() - union_start

print("FinalTable: " + unionTable)
print("union time: " + str(union_time))
print("load time: " + str(load_time))
print("index time: " + str(index_time))
print("total time: " + str(index_time + load_time + union_time))

output = op.tableMeta(unionTable)
srcRows = sum([ output.metas[ii].numRows for ii in range(output.numMetas)])
print("Final table has " + str(srcRows) + " rows")

