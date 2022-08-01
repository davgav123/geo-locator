import sbt._

class Dependencies(versions: Versions) {
  val spark_core = "org.apache.spark" %% "spark-core" % versions.spark % "provided"
  val spark_sql = "org.apache.spark" %% "spark-sql" % versions.spark % "provided"
}
