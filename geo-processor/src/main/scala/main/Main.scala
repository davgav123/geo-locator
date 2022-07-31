package main

import geo.GeoDataProcessor._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, size}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .master("local")
      .appName("geo-master")
      .getOrCreate()

    processGeoData(
      spark,
      "/path/to/data/",
      // for path on EMR you can use s3://...
      // https://codetinkering.com/python-read-write-file-s3-apache-spark-aws-emr/
      // https://docs.amazonaws.cn/en_us/emr/latest/ManagementGuide/emr-plan-upload-s3.html
      europeanCountries = true
    )

    println("done!")

    spark.close()
  }
}
