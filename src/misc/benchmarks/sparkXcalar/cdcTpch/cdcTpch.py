#!/usr/bin/env python

appName = "Modified TPCH Query with CDC"
inputDir = "/user/blim/tpch_benchmark/sf100Headers"
lineItemSrc = inputDir + "/lineitem/lineitem.tbl*"
numDays = 4
lineItemUpdatesSrc = []
for ii in xrange(numDays):
    lineItemUpdatesSrc.append(inputDir + ("/lineItemUpdates/%d/*" % (ii + 1)))
primaryKeys = ["L_ORDERKEY", "L_LINENUMBER"]
timeStampCol = "L_TIMESTAMP"
timeStampIntCol = ("%sInt" % timeStampCol)

# unionJoin or bogus
cdcStrategy="unionJoin"

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as func
from pyspark.sql.functions import lit, col, udf

spark = SparkSession.builder.appName(appName).getOrCreate()

# UDFs required
def udfSimFn(a, b, c):
    return str(c)

udfSim = udf(udfSimFn, StringType())

# First we load all the datasets we need
lineitem = spark.read.format("CSV").option("header", "true").option("delimiter", "|").load(lineItemSrc)
lineitem.createOrReplaceTempView("lineitem")
spark.sql("CACHE TABLE lineitem")

lineItemUpdates = []
lineItemUpdatesTblNames = []
for ii in xrange(numDays):
    lineItemChange = spark.read.format("CSV").option("header", "true").option("delimiter", "|").load(lineItemUpdatesSrc[ii])
    lineItemChangeTblName = "lineItemChange%d" % ii
    lineItemUpdatesTblNames.append(lineItemChangeTblName)
    lineItemChange.createOrReplaceTempView(lineItemChangeTblName)
    spark.sql("CACHE TABLE %s" % lineItemChangeTblName)
    lineItemUpdates.append(lineItemChange)

# Initialize srcTable with timestamp col of -1
srcTable = lineitem.drop("_c16").withColumn(timeStampIntCol, lit(-1))

def keepLatestRecord(table):
    maxTimeStampCol = "%sMax" % timeStampIntCol

    # SELECT max(L_TIMESTAMPInt) as L_TIMESTAMPIntMax FROM table GROUP BY L_ORDERKEY, L_LINENUMBER
    latest = table.groupBy(primaryKeys).agg(func.max(timeStampIntCol).alias(maxTimeStampCol))

    # SELECT * FROM table INNER JOIN latest WHERE table.L_ORDERKEY = latest.L_ORDERKEY AND table.L_LINENUMBER = latest.L_LINENUMBER AND table.L_TIMETAMPInt = latest.L_TIMESTAMPIntMax
    joinConditions = [ table[key] == latest[key] for key in primaryKeys ]
    joinConditions.append(table[timeStampIntCol] == latest[maxTimeStampCol])
    return table.join(latest, joinConditions, "inner").select([table[colName] for colName in table.columns])

def performDayQuery(srcTable):
    answer = srcTable.groupBy("L_ORDERKEY").agg(func.avg("L_QUANTITY").alias("AvgQty")).select(col("L_ORDERKEY").alias("key"), "AvgQty").join(srcTable, col("key") == srcTable["L_ORDERKEY"]).filter(udfSim(col("L_QUANTITY"), col("AvgQty"), col("L_ORDERKEY")) == 1)
    return answer
    
# Now we perform the query
for ii in xrange(numDays):

    # Merge all modifications to the same lineItem into one entry
    lineItemUpdate = keepLatestRecord(lineItemUpdates[ii].withColumn(timeStampIntCol, col(timeStampCol).cast("integer")).drop(timeStampCol))

    # Now use this to update the sourceTable
    if cdcStrategy == "unionJoin":
        srcTable = keepLatestRecord(srcTable.union(lineItemUpdate))
    else:
        srcTable = srcTable.union(lineItemUpdate).dropDuplicates(primaryKeys)

    # We're ready to perform some queries on today's table
    answerTable = performDayQuery(srcTable)
    answerTable.show()

    # Even with srcTable.cache(), Spark's performance was still non-linearly bad wrt to adding more days. And it crashed in the end due to OOM
    # srcTable.cache()
