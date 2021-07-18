package main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.BooleanType

import scala.collection.mutable

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .master("local")
      .appName("geo-master")
      .getOrCreate()

    val points: DataFrame = countryPoints(spark, "serbia")
    points.sample(0.2).show(50)

    spark.close()
  }

  def countryPoints(spark: SparkSession, targetCountry: String): DataFrame = {
    val filePath: String = getClass.getResource("/" + targetCountry + "-latest.osm.pbf").getPath

    val borders = GeoNamesPreprocessor.prepareBorderData(spark)
      .where("Country = \'" + targetCountry.head.toUpper + targetCountry.tail + "\'")

    val polygonType: String = borders.first.getAs("type").toString
    if (polygonType == "Polygon") {
      val polygon: Array[Array[Double]] = borders
        .first
        .getAs("coordinates")
        .asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[String]]]
        .array
        .map(line => line.array.map(e => e.toDouble))

      val polygonContains =
        (lat: Double, lon: Double) => {
          val bordersPolygon: Polygon = new Polygon(polygon)
          bordersPolygon.containsPoint_RayCasting(Array(lon, lat))
        }

      pointsOfCountry(
        spark,
        filePath,
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
        (lat: Double, lon: Double) => {
          val polygons = for (poly <- multipolygon) yield new Polygon(poly)
          polygons.exists(polygon => polygon.containsPoint_RayCasting(Array(lon, lat)))
        }

      pointsOfCountry(
        spark,
        filePath,
        multipolygonContains
      )
    }
  }

  def pointsOfCountry(spark: SparkSession, filePath: String, containsFunction: (Double, Double) => Boolean): DataFrame = {
    val countrySampled = OSMDataPreprocessor.getNodes(spark, filePath)
      .sample(0.2)
      .cache()

    val containsUdf = udf(containsFunction, BooleanType)

    val countryPoints = countrySampled
      .withColumn("belongs_to_country", containsUdf(col("latitude"), col("longitude")))
      .select(col("longitude"), col("latitude"), col("belongs_to_country"))

    countryPoints
  }
}
