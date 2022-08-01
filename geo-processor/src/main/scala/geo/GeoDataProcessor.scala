package geo

import org.apache.spark.sql.functions.{array_intersect, col, expr, flatten, lit, lower, monotonically_increasing_id, size, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, ArrayType}
import utils.Polygon

import scala.collection.mutable

object GeoDataProcessor {
  val dataPath: String = "s3a://geo-master-496542722941/osm-data/countries/"

  def processGeoData(spark: SparkSession, pathToDataFile: String, europeanCountries: Boolean): Unit = {
    val mapped = mapCoordinatesToCountry(spark, pathToDataFile).cache()

    val countryNames: Array[String] = if (europeanCountries) {
      getEuropeanCountries
    } else {
      getAllCountries(spark)
    }

    for (name <- countryNames) {
      val countryDf = mapped
        .where(s"belongs_to == \'$name\'")

      filterCountry(countryDf, name)
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

    val belongsToCountry = udf[String, Double, Double](belongsToCountryFunction)

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
    val pathToBordersFile = "s3a://geo-master-496542722941/geo-names/shapes_all_low.txt" // put s3 path
    val pathToInfoFile = "s3a://geo-master-496542722941/geo-names/country_info.txt" // put s3 path

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
    polygons.exists(polygon => polygon.containsPointSumOfAngles(Array(lon, lat)))
  }

  def getNodes(spark: SparkSession, pathToCountryFile: String): DataFrame = {
    val nodes: DataFrame = spark.read
      .format("parquet")
      .load(pathToCountryFile)

    nodes.select(
      col("latitude"),
      col("longitude"),
      col("tags")
    )
      .where("latitude is not null and longitude is not null")
      .where(size(col("tags")) =!= 0)
  }

  private def getEuropeanCountries: Array[String] = {
    Array(
      "russia", "germany", "united kingdom", "france", "italy", "spain", "ukraine",
      "poland", "romania", "netherlands", "belgium", "czechia", "greece", "portugal",
      "sweden", "hungary", "belarus", "austria", "serbia", "switzerland", "bulgaria",
      "denmark", "finland", "slovakia", "norway", "ireland", "croatia", "moldova",
      "bosnia and herzegovina", "albania", "lithuania", "north macedonia", "slovenia",
      "latvia", "estonia", "montenegro", "luxembourg", "malta", "iceland", "andorra",
      "monaco", "liechtenstein", "san marino", "vatican"
    )
  }

  private def getAllCountries(spark: SparkSession): Array[String] = {
    prepareBorderData(spark)
      .rdd.collect().map(row => row(0).toString)
  }

  def filterCountry(countryDF: DataFrame, countryName: String): Unit = {
    val conditions: Array[String] = Array(
      "religion", "health", "hotel", "hostel", "bar", "nightclub",
      "pub", "restaurant", "parking", "gas", "market"
    )

    val countryDfTags = countryDF
      .cache()

    val removeSpaces = countryName.replace(' ', '-')
    for (condition <- conditions) {
      filterCountryByCondition(countryDfTags, condition)
        .write
        .parquet(dataPath + s"$removeSpaces-$condition")
    }
  }

  def filterCountryByCondition(country: DataFrame, condition: String): DataFrame = {
    val filters = mapCondition(condition)

    country
      .withColumn(
        "tag_values",
        expr("transform(tags, x -> x.value)").cast(ArrayType(StringType))
      )
      .where(size(array_intersect(col("tag_values"), lit(filters))) =!= 0)
      .select(col("latitude"), col("longitude"))
  }

  private def mapCondition(condition: String): Array[String] = {
    condition match {
      case "religion" => Array("church", "place_of_worship", "monastery", "christian", "muslim", "religion")
      case "health" => Array("hospital", "health", "ambulance", "pharmacy", "healthcare")
      case "hotel" => Array("hotel")
      case "hostel" => Array("hostel")
      case "bar" => Array("bar", "cafe")
      case "nightclub" => Array("nightclub")
      case "pub" => Array("pub")
      case "restaurant" => Array("restaurant")
      case "market" => Array("market", "supermarket")
      case "gas" => Array("fuel", "gas", "petrol")
      case "parking" => Array("parking")
      case _ => Array("unknown")
    }
  }

  private def getAllCoordinates(countryDF: DataFrame): DataFrame = {
    countryDF
      .select(col("latitude"), col("longitude"))
  }
}
