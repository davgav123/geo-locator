package restServer

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn

object akkaHttpServer {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "my-system")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext

    val route: Route = path("selection") {
      concat {
        cors() {
          get {
            // http://localhost:8080/selection?country=[COUNTRY]&param=[PARAM]
            parameters('country.as[String], 'param.as[String]) { (country, param) =>
              complete {
                HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h2>Selected: country = $country param = $param</h2>")
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
      .onComplete(_ => system.terminate())
  }
}
