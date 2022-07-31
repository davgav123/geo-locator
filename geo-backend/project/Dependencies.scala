import sbt._

class Dependencies(versions: Versions) {
  val akka_actor = "com.typesafe.akka" %% "akka-actor-typed" % versions.akka
  val akka_stream = "com.typesafe.akka" %% "akka-stream" % versions.akka
  val akka_http = "com.typesafe.akka" %% "akka-http" % versions.akka_http
  val akka_http_spray = "com.typesafe.akka" %% "akka-http-spray-json" % versions.akka_http
  val akka_cors = "ch.megard" %% "akka-http-cors" % versions.akka_cors
  val spark_core = "org.apache.spark" %% "spark-core" % versions.spark
  val spark_sql = "org.apache.spark" %% "spark-sql" % versions.spark
}
