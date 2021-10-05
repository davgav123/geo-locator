package main

import geo.GeoDataProcessor._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .master("local")
      .appName("geo-master")
      .getOrCreate()

    prepareBorderData(spark).show()

    spark.close()
  }
}
