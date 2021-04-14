package Streamming

import org.apache.spark.sql.SparkSession
import common._
import org.apache.spark.sql.types.{IntegerType, StructType}

import java.io
object StreamingFolder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Scala_Project")
      .master("local[2]")
      .getOrCreate()

    val df = spark.readStream
      .format("csv")
      .option("header", false)
      .schema(dataSchema)
      .load(args(1))
    val segment = spark.read.csv(args(0) + "/SEGMENT.csv")
    val rules = spark.read.csv(args(0) + "/RULES.CSV")
    val seg_join = df.join(segment, segment("_c0") === df("Msisdn"))
    val rul_join = df.join(rules, rules("_c0") === seg_join("Service_Identifier")).filter("Traffic_Volume >= _c4")
    val tim_filter = rul_join.withColumn("hour", rul_join("Time_Stamp").substr(9, 2))
    val filter = tim_filter.filter(tim_filter("hour").between(" _c2 ", "_c3")).select("Msisdn", "Service_Identifier", "Traffic_Volume", "Time_stamp", ("_c1")).distinct()
    val finalfile = new io.File(args(2) + "/output")
    finalfile.mkdir()

    val checkkpoint = new io.File(args(2) + "/checkpoint")
    checkkpoint.mkdir()

    val outpath = finalfile.getAbsolutePath
    val checkpath = checkkpoint.getAbsolutePath

    val write = filter.coalesce(1).writeStream
      .format("csv")
      .option("format", "append")
      .option("path", outpath)
      .option("checkpointLocation", checkpath)
      .partitionBy("_c1")
      .outputMode("append")
      .start()
      .awaitTermination()

  }
}
