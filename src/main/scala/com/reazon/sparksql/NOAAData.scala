package com.reazon.sparksql

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object NOAAData {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("NOAA Data DATA YT MARK3").getOrCreate()

    //    in order to evade unnecessary info logs
    spark.sparkContext.setLogLevel("WARN")

    //  importing the implicits for reference
    import spark.implicits._

    //    creating schema for DataFrame for formatting the Data
    val tschema = StructType(Array(
      StructField("sid", StringType),
      StructField("date", DateType),
      StructField("mtype", StringType),
      StructField("value", DoubleType)
    ))

    val data2017: DataFrame = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("C:\\Users\\ayush\\hadoop-3.2.2\\bin\\2017.csv")

    //    data2017.show()
    //    data2017.schema.printTreeString()

    //    limited the data for getting output with minimal configuration
    val tmax2017 = data2017.filter($"mtype" === "TMAX")
      .limit(1000).drop("mytpe")
      .withColumnRenamed("value", "tmax")

    val tmin2017 = data2017.filter('mtype === "TMIN")
      .limit(1000).drop("mytpe")
      .withColumnRenamed("value", "tmin")

    //    joining temps together
    val combinedTemps2017 = tmax2017.join(tmin2017, Seq("sid", "date"))
    combinedTemps2017.show()

    spark.stop()

  }
}
