package restServer

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn

import spray.json._
import DefaultJsonProtocol._

object akkaHttpServer {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("geo-backend")
      .getOrCreate()

    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "my-system")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext

    val route: Route = path("selection") {
      concat {
        cors() {
          get {
            // http://localhost:8080/selection?country=[COUNTRY]&param=[PARAM]
            parameters('country.as[String], 'param.as[String]) { (country, param) =>
              complete {
                val coords = readData(spark, country, param)

                HttpEntity(ContentTypes.`application/json`, s"""${coords.toJson}""")
              }
            }
          }
        }
      }
    }

    val bindingFuture = Http().newServerAt("localhost", 8080).bind(route)

    println(s"Server now online\nPress RETURN to stop...")
    StdIn.readLine()
    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => {
        spark.close()
        system.terminate()
      })
  }

  def readData(spark: SparkSession, country: String, cond: String): Array[Array[Double]] = {
    val countryFilePath = s"path/to/data/$country-$cond/"

    val data = spark.read.parquet(countryFilePath)
      .collect()
      .map(row => Array(row(0).asInstanceOf[Double], row(1).asInstanceOf[Double]))

    data
  }
}
