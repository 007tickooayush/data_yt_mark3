package com.reazon.sparksql

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object NOAAData {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("NOAA Data DATA YT MARK3").getOrCreate()

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

    data2017.show()
//    data2017.schema.printTreeString()


    spark.stop()

  }
}
