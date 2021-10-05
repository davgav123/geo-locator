name := "geo-processor"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5"
)

libraryDependencies += "com.wolt.osm" % "parallelpbf" % "0.3.1"
libraryDependencies += "com.wolt.osm" %% "spark-osm-datasource" % "0.3.0"

