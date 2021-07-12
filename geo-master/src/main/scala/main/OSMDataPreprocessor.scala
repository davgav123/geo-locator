package main

import com.wolt.osm.spark.OsmSource.OsmSource
import main.GeoNamesPreprocessor.prepareBorderData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object OSMDataPreprocessor {
  def getNodes(spark: SparkSession, pathToCountryFile: String): DataFrame = {
    val osm: DataFrame = spark.read
      .format(OsmSource.OSM_SOURCE_NAME)
      .load(pathToCountryFile)

    val nodes: DataFrame = osm.select(
      col("LAT").as("latitude"),
      col("LON").as("longitude"),
      col("TAG").as("tag")
    )

    nodes
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .master("local")
      .appName("geo-master")
      .getOrCreate()

    val df: DataFrame = getNodes(
      spark,
      getClass.getResource("/serbia-latest.osm.pbf").getPath
    )
    df.show()

    spark.close()
  }
}
