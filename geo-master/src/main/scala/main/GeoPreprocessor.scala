package main

import com.wolt.osm.spark.OsmSource.OsmSource
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, flatten, lit, lower, monotonically_increasing_id, udf}
import org.apache.spark.sql.types.StringType

import scala.collection.mutable

object GeoPreprocessor {
  def prepareBorderData(spark: SparkSession): DataFrame = {
    val pathToBordersFile = getClass.getResource("/shapes_all_low.txt").getPath
    val pathToInfoFile = getClass.getResource("/country_info.txt").getPath

    val countryBordersRaw: DataFrame = spark
      .read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(pathToBordersFile)

    import spark.implicits._ // need this in order to cast json to string
    val parsedJson: DataFrame = spark.read.json(countryBordersRaw.select(col("geoJSON")).as[String])
      .withColumn("join_column", monotonically_increasing_id())

    val prepareForJoin: DataFrame = countryBordersRaw
      .withColumn("join_column", monotonically_increasing_id())
      .drop(col("geoJSON"))

    val countryBorders: DataFrame = prepareForJoin
      .join(parsedJson, "join_column")
      .select(
        col("geoNameId"),
        col("type"),
        flatten(col("coordinates")).as("coordinates")
      )

    val countryToId: DataFrame = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(pathToInfoFile)
      .select(col("geonameid").as("id"), col("Country"))

    val borders: DataFrame = countryBorders
      .join(
        countryToId,
        countryBorders("geoNameId") === countryToId("id"),
        "inner"
      )
      .select(
        lower(col("Country")).as("country"),
        col("type"),
        col("coordinates")
      )

    borders
  }

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

  def attachPointsToCountry(spark: SparkSession): DataFrame = {
    val borders = GeoNamesPreprocessor.prepareBorderData(spark)

    var osmData: DataFrame = getNodes(spark, getClass.getResource("path_to_OSM_file").getPath)
      .sample(0.20) // sample for performance reasons
      .withColumn("belongs_to", lit("x"))

    for (country <- borders.collect().map(row => row(0).toString)) {
      osmData = locatePoints(
        country,
        borders.where("lower(Country) = \'" + country + "\'"),
        osmData
      )
    }

    osmData = osmData
      .where("belongs_to != \'x\'")
      .cache()

    osmData
  }

  def locatePoints(targetCountry: String, borders: DataFrame, data: DataFrame): DataFrame = {
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
        data,
        multipolygonContains
      )
    }
  }

  private def pointsOfCountry(data: DataFrame, containsFunction: (Double, Double, String) => String): DataFrame = {
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

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .master("local")
      .appName("geo-master")
      .getOrCreate()

    prepareBorderData(spark)

    getNodes(
      spark,
      getClass.getResource("/serbia-latest.osm.pbf").getPath
    ).sample(0.1)

    spark.close()
  }
}
