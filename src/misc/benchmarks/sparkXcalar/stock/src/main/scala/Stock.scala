package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer

object Stock {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Stock Benchmark")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits.
_
    val inputDir = "/user/blim/stock_sim"

    val df1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(inputDir + "/sim_0/*.csv")
    val df2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(inputDir + "/sim_1/*.csv")
    val df3 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(inputDir + "/sim_2/*.csv")
    val df4 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(inputDir + "/sim_3/*.csv")
    val df5 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(inputDir + "/sim_4/*.csv")
    val df6 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(inputDir + "/sim_5/*.csv")
    val df7 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(inputDir + "/sim_6/*.csv")
    val df8 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(inputDir + "/sim_7/*.csv")

    df1.show()
 
    val joinedDf = df1.join(df2.withColumnRenamed("msecs", "d2Msecs").withColumnRenamed("ticker", "d2Ticker"), $"msecs" === $"d2Msecs" && $"ticker" === $"d2Ticker", "fullouter")
                      .join(df3.withColumnRenamed("msecs", "d3Msecs").withColumnRenamed("ticker", "d3Ticker"), $"msecs" === $"d3Msecs" && $"ticker" === $"d3Ticker", "fullouter")
                      .join(df4.withColumnRenamed("msecs", "d4Msecs").withColumnRenamed("ticker", "d4Ticker"), $"msecs" === $"d4Msecs" && $"ticker" === $"d4Ticker", "fullouter") 
                      .join(df5.withColumnRenamed("msecs", "d5Msecs").withColumnRenamed("ticker", "d5Ticker"), $"msecs" === $"d5Msecs" && $"ticker" === $"d5Ticker", "fullouter") 
                      .join(df6.withColumnRenamed("msecs", "d6Msecs").withColumnRenamed("ticker", "d6Ticker"), $"msecs" === $"d6Msecs" && $"ticker" === $"d6Ticker", "fullouter") 
                      .join(df7.withColumnRenamed("msecs", "d7Msecs").withColumnRenamed("ticker", "d7Ticker"), $"msecs" === $"d7Msecs" && $"ticker" === $"d7Ticker", "fullouter") 
                      .join(df8.withColumnRenamed("msecs", "d8Msecs").withColumnRenamed("ticker", "d8Ticker"), $"msecs" === $"d8Msecs" && $"ticker" === $"d8Ticker", "fullouter") 


    joinedDf.show()
  }
}
