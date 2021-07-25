package OSMDemoOnSubset

import main.GeoPreprocessor
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Create a subset of OSMData that will be used for checking how app works on local machine
 * subset will contain following countries, because they are not large and are neighbors:
 * - Slovenia
 * - Croatia
 */

object Demo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .master("local")
      .appName("geo-subset-master")
      .getOrCreate()

    val borders = GeoPreprocessor.prepareBorderData(spark)
      .where("lower(Country) in (\'slovenia\', \'croatia\')")

    var osmData: DataFrame = createSubset(spark)
      .sample(0.10) // sample for performance reasons
      .withColumn("belongs_to", lit("x"))

    for (country <- Array("slovenia", "croatia")) {
      osmData = GeoPreprocessor.locatePoints(
        country,
        borders.where("lower(Country) = \'" + country + "\'"),
        osmData
      )
    }

    osmData = osmData
      .where("belongs_to != \'x\'")
      .cache()

    print("total points: ")
    println(osmData.count())
    print("Croatia points: ")
    println(osmData.where("belongs_to = \'croatia\'").count())
    print("Slovenia points: ")
    println(osmData.where("belongs_to = \'slovenia\'").count())

    spark.close()
  }

  def createSubset(spark: SparkSession): DataFrame = {
    val sloveniaNodes = GeoPreprocessor.getNodes(
      spark,
      getClass.getResource("/slovenia-latest.osm.pbf").getPath
    )

    val croatiaNodes = GeoPreprocessor.getNodes(
      spark,
      getClass.getResource("/croatia-latest.osm.pbf").getPath
    )

    sloveniaNodes.union(croatiaNodes)
  }
}
