package OSMDemoOnSubset

import main.{GeoNamesPreprocessor, OSMDataPreprocessor, Polygon}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

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

    val borders = GeoNamesPreprocessor.prepareBorderData(spark)
      .where("lower(Country) in (\'slovenia\', \'croatia\')")

    var osmData: DataFrame = createSubset(spark)
      .sample(0.1) // sample for performance reasons
      .withColumn("belongs_to", lit("x"))

    for (country <- Array("slovenia", "croatia")) {
      osmData = locatePoints(
        spark,
        country,
        borders.where("lower(Country) = \'" + country + "\'"),
        osmData
      )
    }

    osmData = osmData.sample(0.05).cache()
    println(osmData.count())
    println(osmData.where("belongs_to = \'croatia\'").count())
    println(osmData.where("belongs_to = \'slovenia\'").count())

    osmData.where("belongs_to = \'croatia\'").show()
    osmData.where("belongs_to = \'slovenia\'").show()

    spark.close()
  }

  def createSubset(spark: SparkSession): DataFrame = {
    val sloveniaNodes = OSMDataPreprocessor.getNodes(
      spark,
      getClass.getResource("/slovenia-latest.osm.pbf").getPath
    )

    val croatiaNodes = OSMDataPreprocessor.getNodes(
      spark,
      getClass.getResource("/croatia-latest.osm.pbf").getPath
    )

    sloveniaNodes.union(croatiaNodes)
  }

  def locatePoints(spark: SparkSession, targetCountry: String, borders: DataFrame, data: DataFrame): DataFrame = {
    val polygonType: String = borders.first.getAs("type").toString
    if (polygonType == "Polygon") {
      val polygon: Array[Array[Double]] = borders
        .first
        .getAs("coordinates")
        .asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[String]]]
        .array
        .map(line => line.array.map(e => e.toDouble))

      val polygonContains =
        (lat: Double, lon: Double, belongsTo: String) => {
          if (belongsTo.equals("x")) {
            val bordersPolygon: Polygon = new Polygon(polygon)
            if (bordersPolygon.containsPoint_RayCasting(Array(lon, lat)))
              targetCountry
            else
              "x"
          } else {
            belongsTo
          }
        }

      pointsOfCountry(
        spark,
        data,
        polygonContains
      )

    } else {
      val multipolygon: Array[Array[Array[Double]]] = borders
        .first
        .getAs("coordinates")
        .asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[String]]]
        .array
        .map(polygon => polygon
          .array
          .map(line => line
            .split(' ')
            .flatMap(e => e
              .filterNot(c => "[]".contains(c)).split(',').map(s => s.toDouble))))

      val multipolygonContains =
        (lat: Double, lon: Double, belongsTo: String) => {
          if (belongsTo.equals("x")) {
            val polygons = for (poly <- multipolygon) yield new Polygon(poly)
            if (polygons.exists(polygon => polygon.containsPoint_RayCasting(Array(lon, lat))))
              targetCountry
            else
              "x"
          } else {
            belongsTo
          }
        }

      pointsOfCountry(
        spark,
        data,
        multipolygonContains
      )
    }
  }

  def pointsOfCountry(spark: SparkSession, data: DataFrame, containsFunction: (Double, Double, String) => String): DataFrame = {
    val containsUdf = udf(containsFunction, StringType)

    val countryPoints = data
      .withColumn("belongs_to_country", containsUdf(
        col("latitude"),
        col("longitude"),
        col("belongs_to"))
      )
      .select(
        col("longitude"),
        col("latitude"),
        col("belongs_to_country").as("belongs_to")
      )

    countryPoints
  }
}
