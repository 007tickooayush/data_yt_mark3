package com.reazon.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{Encoders, SparkSession}

case class Series(id: String, are: String, measure: String, title: String)

case class LAData(id: String, year: Int, period: String, value: Double)

object BLSTyped {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("NOAA Data DATA YT MARK3").getOrCreate()

    val sc = spark.sparkContext

    //    in order to evade unnecessary info logs
    sc.setLogLevel("WARN")

    //    to suppress BlockManager Exceptions while execution
    Logger.getLogger("org").setLevel(Level.FATAL)

    //  importing the implicits for reference
    import spark.implicits._


    // LINK BLS Data:
    // https://download.bls.gov/pub/time.series/la/
    sc.addFile("src\\data\\la.data.64.County.txt")

    val countyData = spark.read
      .schema(Encoders.product[LAData].schema) // fetch the schema for acccurate mapping
      .option("header", value = true)
      .option("delimiter", "\t")
      .csv(SparkFiles.get("la.data.64.County.txt"))
      .as[LAData] // import the data as specified in Case Class

    countyData.show()

    spark.stop()
  }
}
