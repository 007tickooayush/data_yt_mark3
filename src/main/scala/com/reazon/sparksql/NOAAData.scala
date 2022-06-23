package com.reazon.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object NOAAData {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("NOAA Data DATA YT MARK3").getOrCreate()

    val sc = spark.sparkContext

    //    in order to evade unnecessary info logs
    sc.setLogLevel("WARN")

    //    to suppress BlockManager Exceptions while execution
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

    //LINK:
    // https://www.ncei.noaa.gov/pub/data/ghcn/daily/
    sc.addFile("src\\data\\2017.csv")
    val data2017: DataFrame = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv(SparkFiles.get("2017.csv"))
    //      .csv("src\\data\\2017.csv")

    //    data2017.show()
    //    data2017.schema.printTreeString()

    //    sschema for station data
    val sschema = StructType(Array(
      StructField("sid", StringType),
      StructField("lat", DoubleType),
      StructField("lon", DoubleType),
      StructField("name", StringType)
    ))

    sc.addFile("src\\data\\ghcnd-stations.txt")
    val stationRDD = sc.textFile(SparkFiles.get("ghcnd-stations.txt")).map { line =>
      val sid = line.substring(0, 11)
      val lat = line.substring(12, 20).toDouble
      val lon = line.substring(21, 30).toDouble
      val name = line.substring(41, 71)
      Row(sid, lat, lon, name)
    }

    //    using the custom created schema convert the RDD[Row] to DataFrame
    val stations = spark.createDataFrame(stationRDD, sschema).cache()

    //    putting the data2017 as SQL View since data2017 is not recognizable in SQL (only a scala variable)
    data2017.createOrReplaceTempView("data2017")
    //    testing a pure SQL query
    val pureSQL = spark.sql(
      """
        SELECT sid ,date, value as tmax FROM data2017 where mtype = "TMAX"
        """.stripMargin)
    pureSQL.show()

    //    //    limited the data for getting output with minimal configuration
    //    val tmax2017 = data2017.filter($"mtype" === "TMAX")
    //      .select('sid, 'date, 'value)
    //      .limit(1000)
    //      //      .drop('mytpe)
    //      .withColumnRenamed("value", "tmax")
    //
    //
    //    val tmin2017 = data2017.filter('mtype === "TMIN")
    //      .select('sid, 'date, 'value)
    //      .limit(1000)
    //      //      .drop('mytpe)
    //      .withColumnRenamed("value", "tmin")
    //
    //    //    joining temps together
    //    val combinedTemps2017 = tmax2017.join(tmin2017, Seq("sid", "date"))
    //    //      .select('sid, 'date, 'tmax, 'tmin)
    //    //    combinedTemps2017.show()
    //
    //    //    calculating average temperature for each area code
    //    val dailyTemps2017 = combinedTemps2017.select('sid, 'date, ('tmax + 'tmin) / 20 * 1.8 + 32 as "tavg")
    //    //      .withColumnRenamed("((((tmax + tmin) / 20) * 1.8) + 32)", "tavg")
    //    //    dailyTemps2017.show(20)
    //
    //    val stationTemp2017 = dailyTemps2017.groupBy('sid).agg(avg('tavg) as "tavg")
    //    //    stationTemp2017.show()
    //
    //    //    stations.show()
    //
    //    //    stations.schema.printTreeString()
    //    //    stationTemp2017.schema.printTreeString()
    //    val joinedData2017 = stationTemp2017.join(stations)
    //    joinedData2017.show()

    spark.stop()

  }
}
