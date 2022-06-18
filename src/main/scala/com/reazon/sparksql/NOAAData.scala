package com.reazon.sparksql

import org.apache.spark.sql.SparkSession

object NOAAData {
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("NOAA Data DATA YT MARK3").getOrCreate()

//  importing the implicits for reference
  import spark.implicits._

}
