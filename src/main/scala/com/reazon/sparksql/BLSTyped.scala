package com.reazon.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

case class Series(sid: String, are: String, measure: String, title: String)

case class LAData(id: String, year: Int, period: String, value: Double)

object BLSTyped {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("BLS Data DATA YT MARK4").getOrCreate()

    val sc = spark.sparkContext

    //    in order to evade unnecessary info logs
    sc.setLogLevel("WARN")

    //    to suppress BlockManager Exceptions while execution
    Logger.getLogger("org").setLevel(Level.FATAL)

    //  importing the implicits for reference
    import spark.implicits._


    // LINK BLS Data:
    // https://download.bls.gov/pub/time.series/la/
    sc.addFile("src\\data\\la.data.64.County.csv")

    val countyData = spark.read
      .schema(Encoders.product[LAData].schema) // fetch the schema for acccurate mapping
      .option("header", value = true)
      .option("delimiter", "\t")
      .csv(SparkFiles.get("la.data.64.County.csv"))
      .select(trim('id) as "id", 'year, 'period, 'value)
      .as[LAData] // import the data as specified in Case Class
      .sample(withReplacement = false, 0.1) // sampling data
      .cache()
    //    countyData.show()

    sc.addFile("src\\data\\la.series.tsv")
    val series = spark.read.textFile(SparkFiles.get("la.series.tsv")).map { line =>
      val p = line.split("\t").map(_.trim)
      Series(p(0), p(2), p(3), p(6))
    }.cache()

    //    series.show()

    //    renamed the id to sid in Series and id from LAData using joinWith returns DataSet
    val join1 = countyData.joinWith(series, 'id === 'sid)
    //    join1.show()
    println(join1.first())

    spark.stop()
  }
}
