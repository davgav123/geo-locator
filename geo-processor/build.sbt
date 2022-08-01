name := "geo-processor"

version := "1.0-processor"

scalaVersion := "2.12.12"

val versions = new Versions()
val dependencies = new Dependencies(versions)

libraryDependencies ++= Seq(
  dependencies.spark_core,
  dependencies.spark_sql
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}



