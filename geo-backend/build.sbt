name := "geo-backend"

version := "1.0-backend"

scalaVersion := "2.12.11"

val versions = new Versions()
val dependencies = new Dependencies(versions)


libraryDependencies ++= Seq(
  dependencies.akka_actor,
  dependencies.akka_stream,
  dependencies.akka_http,
  dependencies.akka_http_spray,
  dependencies.akka_cors
)

libraryDependencies ++= Seq(
  dependencies.spark_core,
  dependencies.spark_sql
)

assemblyMergeStrategy in assembly := {
  case PathList("reference.conf") => MergeStrategy.concat
  case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF",xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
