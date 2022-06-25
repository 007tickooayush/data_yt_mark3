package com.reazon.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkContext, SparkFiles}


case class CountyWeight(county_fips: Map[String, Double])

case class ZipData(
                    zip: String,
                    lat: Double,
                    lng: Double,
                    city: String,
                    state_id: String,
                    state_name: String,
                    zcta: Boolean,
                    parent_zcta: String,
                    population: Integer,
                    density: Double,
                    county_fips: Integer,
                    county_name: String,
                    county_weights: String, // need nested mapping for accuracy
                    county_names_all: String,
                    county_fips_all: String,
                    imprecise: Boolean,
                    military: Boolean,
                    timezone: String
                  )

object Demo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("DEMO Data Testing DATA YT MARK5").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //    in order to evade unnecessary info logs
    sc.setLogLevel("WARN")

    //    to suppress BlockManager Exceptions while execution
    Logger.getLogger("org").setLevel(Level.FATAL)

    //  importing the implicits for reference
    import spark.implicits._

    sc.addFile("src\\data\\uszips_raw.csv")
    val zipData = spark.read
      .option("header", value = true)
      //      .option("quoteAll", value = true)
      .option("escape", "\"")
      .schema(Encoders.product[ZipData].schema)
      //      .schema(zipSchema)
      .csv(SparkFiles.get("uszips_raw.csv"))
      .filter('lat.isNotNull)
      .as[ZipData]
      .cache()

    //    zipData.show()
    //    zipData.schema.printTreeString()

    val mappedWeight = zipData.select("county_weights").map { row =>
      val arr = row.toString()
        .replace("\"", "")
        .replaceAll("[\\[\\](){}]", "")
        .split("\\s*,\\s*")

      val regex = "\\s*:\\s*"
      val weights = arr.map {data =>
        val fip = data.split(regex)(0)
        val weight = data.split(regex)(1)

        (fip,weight)
      }.toSeq

      weights
    }

    println(mappedWeight.first())

    //    when writing JSON inside csv always use escape sequence
    //    frame.write.option("quoteAll","true").option("escape", "\"").csv("csvFileName")

    spark.stop()
  }
}