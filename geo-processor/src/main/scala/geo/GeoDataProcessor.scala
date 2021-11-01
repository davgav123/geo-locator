package geo

import com.wolt.osm.spark.OsmSource.OsmSource
import org.apache.spark.sql.functions.{array_intersect, col, flatten, lit, lower, map_values, monotonically_increasing_id, size, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StringType
import utils.Polygon

import scala.collection.mutable

object GeoDataProcessor {
  val dataPath: String = "/path/to/data/dir/"

  def processGeoData(spark: SparkSession, pathToDataFile: String): Unit = {
    val mapped = mapCoordinatesToCountry(spark, pathToDataFile)

    val countryNames: Array[String] = prepareBorderData(spark)
      .rdd.collect().map(row => row(0).toString)

    for (name <- countryNames) {
      val countryDf = mapped
        .where(s"belongs_to == \'$name\'")

      filterCountry(countryDf, name)

      getAllCoordinates(countryDf)
        .write
        .parquet(dataPath + s"$name-everything")
    }
  }

  def mapCoordinatesToCountry(spark: SparkSession, countryFilePath: String): DataFrame = {
    val iterableBorders = makeBordersIterable(prepareBorderData(spark))

    val belongsToCountryFunction: (Double, Double) => String = (lat: Double, lon: Double) => {
      var belongs_to = "-"
      for ((country, borders) <- iterableBorders) {
        if (isInsideBorder(lat, lon, borders)) {
          belongs_to = country
        }

      }

      belongs_to
    }

    val belongsToCountry = udf(belongsToCountryFunction, StringType)

    val osmData = getNodes(spark, countryFilePath)
      .withColumn(
        "belongs_to",
        belongsToCountry(
          col("latitude"),
          col("longitude")
        )
      )

    osmData
  }

  private def prepareBorderData(spark: SparkSession): DataFrame = {
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

  private def getNodes(spark: SparkSession, pathToCountryFile: String): DataFrame = {
    val osm: DataFrame = spark.read
      .format(OsmSource.OSM_SOURCE_NAME)
      .load(pathToCountryFile)
//      .sample(0.01) // TODO just for testing, should be removed

    val nodes: DataFrame = osm.select(
      col("LAT").as("latitude"),
      col("LON").as("longitude"),
      col("TAG").as("tag")
    )
      .where("latitude is not null and longitude is not null")

    nodes
  }

  def filterCountry(countryDF: DataFrame, countryName: String): Unit = {
    val conditions: Array[String] = Array(
      "religion", "village", "hotel", "hotel_hostel", "bars",
      "restaurant", "health", "market", "gas", "parking", "culture"
    )

    val countryDfTags = countryDF
      .withColumn("tag_values", map_values(col("tag")))
      .where(size(col("tag_values")) =!= 0)
      .cache()

    for (condition <- conditions) {
      filterCountryByCondition(countryDfTags, condition)
        .write
        .parquet(dataPath + s"$countryName-$condition/")
    }
  }

  def filterCountryByCondition(country: DataFrame, condition: String): DataFrame = {
    val filters = mapCondition(condition)

    country
      .where(size(array_intersect(col("tag_values"), lit(filters))) =!= 0)
      .select(col("latitude"), col("longitude"))
  }

  private def mapCondition(condition: String): Array[String] = {
    condition match {
      case "religion" => Array("church", "place_of_worship", "monastery", "christian", "muslim", "religion")
      case "village" => Array("village", "hamlet")
      case "hotel" => Array("hotel")
      case "hotel_hostel" => Array("hostel", "hotel")
      case "bars" => Array("bar", "nightclub", "pub", "cafe")
      case "restaurant" => Array("restaurant")
      case "health" => Array("hospital", "health", "ambulance", "pharmacy")
      case "market" => Array("market", "supermarket")
      case "gas" => Array("fuel", "gas", "petrol")
      case "parking" => Array("parking")
      case "culture" => Array("monument", "museum", "memorial", "library", "castle")
      case _ => Array("unknown")
    }
  }

  private def getAllCoordinates(countryDF: DataFrame): DataFrame = {
    countryDF
      .select(col("latitude"), col("longitude"))
  }
}
