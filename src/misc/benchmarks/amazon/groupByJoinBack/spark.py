inputDir = "tpch-sf100"
lineItemSrc = inputDir + "/lineitem/lineitem.tbl*"

# First we load all the datasets we need
lineitem = spark.read.format("CSV").option("header", "true").option("delimiter", "|").load(lineItemSrc)
lineitem.createOrReplaceTempView("lineitem")
spark.sql("CACHE TABLE lineitem")

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as func
from pyspark.sql.functions import lit, col, udf

# UDFs required
def udfSimFn(a, b, c):
    return str(c)

udfSim = udf(udfSimFn, StringType())

    
t1 = lineitem.groupBy("L_ORDERKEY").agg(func.avg("L_QUANTITY").alias("AvgQty")).select(col("L_ORDERKEY").alias("key"), "AvgQty").join(lineitem, col("key") == lineitem["L_ORDERKEY"]).filter(udfSim(col("L_QUANTITY"), col("AvgQty"), col("L_ORDERKEY")) == 1)

