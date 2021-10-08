package geo

import com.wolt.osm.spark.OsmSource.OsmSource
import org.apache.spark.sql.functions.{col, flatten, lower, monotonically_increasing_id, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StringType
import utils.Polygon

import scala.collection.mutable

object GeoDataProcessor {
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

    val prepareBordersForJoin: DataFrame = countryBordersRaw
      .withColumn("join_column", monotonically_increasing_id())
      .drop(col("geoJSON"))

    val countryBorders: DataFrame = prepareBordersForJoin
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
        col("type").as("border_type"),
        col("coordinates").as("border_coordinates")
      )

    borders
  }

  def getNodes(spark: SparkSession, pathToCountryFile: String): DataFrame = {
    val osm: DataFrame = spark.read
      .format(OsmSource.OSM_SOURCE_NAME)
      .load(pathToCountryFile)
      .sample(0.05) // TODO remove this

    val nodes: DataFrame = osm.select(
      col("LAT").as("latitude"),
      col("LON").as("longitude"),
      col("TAG").as("tag")
    )

    nodes
  }

  def pairCoordinatesAndCountries(spark: SparkSession): DataFrame = {
    val iterableBorders = makeBordersIterable(prepareBorderData(spark))

    val countryFunction: (Double, Double) => String = (lat: Double, lon: Double) => {
      var belongs_to = "-"
      for ((country, borders) <- iterableBorders) {
        if (isInsideBorder(lat, lon, borders)) {
          belongs_to = country
        }

      }

      belongs_to
    }

    val belongsToCountry = udf(countryFunction, StringType)

    val osmData = getNodes(spark, "add path") // TODO add path to SOM file
      .sample(0.1) // sample for performance reasons TODO: remove it
      .withColumn(
        "belongs_to",
        belongsToCountry(
          col("latitude"),
          col("longitude")
        )
      )

    osmData
  }

  private def makeBordersIterable(borders: DataFrame): Array[(String, Array[Array[Array[Double]]])] = {
    borders.rdd.collect().map(row => getBorderPoints(row))
  }

  private def getBorderPoints(row: Row): (String, Array[Array[Array[Double]]]) = {
    if (row(1) == "Polygon") {
      val border = Array(
        row(2)
        .asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[String]]]
        .array
        .map(line => line.array.map(e => e.toDouble))
      )

      Tuple2(row(0).asInstanceOf[String], border)
    } else {
      val border = row(2)
        .asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[String]]]
        .array
        .map(polygon => polygon
          .array
          .map(line => line
            .split(' ')
            .flatMap(e => e
              .filterNot(c => "[]".contains(c)).split(',').map(s => s.toDouble))))

      Tuple2(row(0).asInstanceOf[String], border)
    }
  }

  private def isInsideBorder(lat: Double, lon: Double, borders: Array[Array[Array[Double]]]): Boolean = {
    val polygons = for (polygon <- borders) yield new Polygon(polygon)
    polygons.exists(polygon => polygon.containsPointRayCastingImpl2(Array(lon, lat)))
  }
}
