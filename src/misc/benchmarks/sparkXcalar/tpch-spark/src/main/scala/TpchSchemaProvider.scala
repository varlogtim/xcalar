package main.scala

import org.apache.spark.SparkContext

// TPC-H table schemas
case class Customer(
  c_custkey: Int,
  c_name: String,
  c_address: String,
  c_nationkey: Int,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String)

case class Lineitem(
  l_orderkey: Int,
  l_partkey: Int,
  l_suppkey: Int,
  l_linenumber: Int,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

case class Nation(
  n_nationkey: Int,
  n_name: String,
  n_regionkey: Int,
  n_comment: String)

case class Order(
  o_orderkey: Int,
  o_custkey: Int,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Int,
  o_comment: String)

case class Part(
  p_partkey: Int,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Int,
  p_container: String,
  p_retailprice: Double,
  p_comment: String)

case class Partsupp(
  ps_partkey: Int,
  ps_suppkey: Int,
  ps_availqty: Int,
  ps_supplycost: Double,
  ps_comment: String)

case class Region(
  r_regionkey: Int,
  r_name: String,
  r_comment: String)

case class Supplier(
  s_suppkey: Int,
  s_name: String,
  s_address: String,
  s_nationkey: Int,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String)

class TpchSchemaProvider(sc: SparkContext, inputDir: String) {

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  // Direct dataframe pattern. Each table below is yielded as a
  // dataframe. This is much faster than the stock package which does RDD ->
  // dataframe conversion first and then invokes createOrReplaceTempView.

  val customer = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").option("header", "true").load(inputDir + "/customer/customer.tbl*")
  val lineitem = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").option("header", "true").load(inputDir + "/lineitem/lineitem.tbl*")
  val nation = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").option("header", "true").load(inputDir + "/nation/nation.tbl*")
  val region = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").option("header", "true").load(inputDir + "/region/region.tbl*")
  val order = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").option("header", "true").load(inputDir + "/orders/orders.tbl*")
  val part = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").option("header", "true").load(inputDir + "/part/part.tbl*")
  val partsupp = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").option("header", "true").load(inputDir + "/partsupp/partsupp.tbl*")
  val supplier = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").option("header", "true").load(inputDir + "/supplier/supplier.tbl*")

  customer.createOrReplaceTempView("customer")
  lineitem.createOrReplaceTempView("lineitem")
  nation.createOrReplaceTempView("nation")
  region.createOrReplaceTempView("region")
  order.createOrReplaceTempView("order")
  part.createOrReplaceTempView("part")
  partsupp.createOrReplaceTempView("partsupp")
  supplier.createOrReplaceTempView("supplier")

  // The following commands execute the actual load into memory and then cache
  // the table. The times reported against these lines in the yarn dashboard
  // are the load times to load each table. This is different from the stock
  // package which doesn't cache the tables and the load times would be
  // included in the query execution itself, as opposed to enabling us to
  // track the load times separately. This is done to make the comparison with
  // Xcalar TPC-H query executions valid, since the Xcalar runs split out
  // the load times separately, and we'd like to compare query execution
  // performance between Xcalar and Spark without load in the picture.

  sqlContext.sql("CACHE TABLE customer")
  sqlContext.sql("CACHE TABLE lineitem")
  sqlContext.sql("CACHE TABLE nation")
  sqlContext.sql("CACHE TABLE region")
  sqlContext.sql("CACHE TABLE order")
  sqlContext.sql("CACHE TABLE part")
  sqlContext.sql("CACHE TABLE partsupp")
  sqlContext.sql("CACHE TABLE supplier")

}
