package com.reazon.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object NOAAData {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("NOAA Data DATA YT MARK3").getOrCreate()

    //    in order to evade unnecessary info logs
    spark.sparkContext.setLogLevel("WARN")
    Logger.getLogger("org").setLevel(Level.FATAL)

    //  importing the implicits for reference
    import spark.implicits._

    //    creating schema for DataFrame for formatting the Data
    val tschema = StructType(Array(
      StructField("sid", StringType),
      StructField("date", DateType),
      StructField("mtype", StringType),
      StructField("value", DoubleType)
    ))

    spark.sparkContext.addFile("src\\data\\2017.csv")
    val data2017: DataFrame = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv(SparkFiles.get("2017.csv"))
    //      .csv("src\\data\\2017.csv")

    //    data2017.show()
    //    data2017.schema.printTreeString()

    //    limited the data for getting output with minimal configuration
    val tmax2017 = data2017.filter($"mtype" === "TMAX")
      .limit(1000)
      .select('sid, 'date, 'value)
//      .drop('mytpe)
      .withColumnRenamed("value", "tmax")


    val tmin2017 = data2017.filter('mtype === "TMIN")
      .limit(1000)
      .select('sid, 'date, 'value)
//      .drop('mytpe)
      .withColumnRenamed("value", "tmin")

    //    joining temps together
    val combinedTemps2017 = tmax2017.join(tmin2017, Seq("sid", "date"))
    //      .select('sid, 'date, 'tmax, 'tmin)
    combinedTemps2017.show()

    spark.stop()

  }
}
