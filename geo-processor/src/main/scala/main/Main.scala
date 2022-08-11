package main

import geo.GeoDataProcessor.processGeoData
import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("geo-master")
      .getOrCreate()

    println("[LOG]: STARTING!")

    processGeoData(
      spark,
      "s3a://geo-master-496542722941/osm-data/europe/europe-latest.osm.pbf.node.parquet",
      "s3a://geo-master-496542722941/osm-data/countries/countries-data"
    )

    println("[LOG]: DONE!")

    spark.close()
  }
}
