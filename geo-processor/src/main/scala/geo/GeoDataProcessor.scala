package geo

import com.wolt.osm.spark.OsmSource.OsmSource
import org.apache.spark.sql.functions.{col, flatten, lower, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    val nodes: DataFrame = osm.select(
      col("LAT").as("latitude"),
      col("LON").as("longitude"),
      col("TAG").as("tag")
    )

    nodes
  }

}
