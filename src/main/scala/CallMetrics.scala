import java.io.{File, PrintWriter}
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsArray, JsObject, JsString, Json}
import play.api.libs.ws.{DefaultBodyReadables, WSAuthScheme}
import play.api.libs.ws.ahc.StandaloneAhcWSClient
import play.api.libs.json._
import play.api.libs.ws.JsonBodyReadables._
import play.api.libs.ws.JsonBodyWritables._
import play.api.libs.ws.DefaultBodyReadables._
import play.api.libs.ws.DefaultBodyWritables._
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.io.StdIn


object CallMetrics extends App  {
  val t = new CallMetrics()
  t.callMetrics()
}

class CallMetrics  {

  implicit val system = ActorSystem()
  system.registerOnTermination {
    System.exit(0)
  }
  implicit val materializer = ActorMaterializer()
  import DefaultBodyReadables._
  import scala.concurrent.ExecutionContext.Implicits._

  val logger = LoggerFactory.getLogger(classOf[CallExportG9])
  val url = "https://pplus.canal-plus.com/awsinfra-metrologie-prod/api/datasources/proxy/3/metrics/expand"
  val urlRender = "https://pplus.canal-plus.com/awsinfra-metrologie-prod/api/datasources/proxy/3/render"
  val cookie = "grafana_user=fguay; grafana_remember=ee2f4a82e0b0edc4af66e19430b779e2d1a3dd06c6f4954a; grafana_sess=22d9ff9e92975e6e; __troRUID=bc0c5b9d-8a83-441b-ad9e-93f0b0a6fadd; s_fid=25C9618E9A373890-3B2BF6C56A5350D4; __trossion=1498748199_1800_2_bc0c5b9d-8a83-441b-ad9e-93f0b0a6fadd%3A1498748199_bc0c5b9d-8a83-441b-ad9e-93f0b0a6fadd%3A1499094309_1499094309_1; p_pass_token=AAAAEItU1xymTXlYdBmCGZ/TZG7sZlUw7FAeoTBEvqJpKXGv+KtMhnFA2ZZLpDS2XkeSIX50gvaN74IxClua8Qqm6QQxm45KZzqtXdMitE8I6p3iw3cEj10PU7qMOIhx6FYTvbt3bob3Pv2/6cTgRKLkTHMZZl8Zzi8/KJaNQsVU7s5I; s_vi=[CS]v1|2C6CBB8805193CA3-4000060E8002137B[CE]; _ga=GA1.2.2144058702.1490646722"

  val wsClient = StandaloneAhcWSClient()


  def callMetrics(): Unit ={

    val writerBroadcast = new PrintWriter(new File("/tmp/metrics.txt"))
    Console.println("Start from metrics :")
    val metric = StdIn.readLine()

    logger.info("Start pooling metrics...")
    val res = callGraphite(writerBroadcast, Seq(metric), Seq())
    res.onFailure{
      case t => {
        logger.error(t.getMessage,t)
      }
    }
    res.onComplete{
      _ => {
        writerBroadcast.close()
        wsClient.close()
        system.terminate()
      }
    }
  }


  def callGraphite(writer:PrintWriter, metrics:Seq[String], acc:Seq[String]): Future[Seq[String]] = {
    val res = metrics.map { m =>

      logger.info(s"Call url metrics for $url?query=$m.*")

      val maybeResp = wsClient.url(url).withMethod("GET")
        .addQueryStringParameters("query" -> (m + ".*"))
        .withRequestTimeout(Duration(100, TimeUnit.SECONDS))
        .addHttpHeaders("Cookie" -> cookie)
        .addHttpHeaders("Authorization" -> "Basic Zmd1YXk6aWVqMTJMZWV4").get()

      val maybeStatusJson = maybeResp.map {
        response => {
          val statusText: String = response.statusText
          val body = response.body[String]
          if (statusText.equals("OK")) {
            val bjson = Json.parse(body)
            val array = (bjson \ "results").asOpt[JsArray].getOrElse(JsArray.empty)
            if (!array.value.isEmpty) {
              (statusText, array)
            } else {
              ("Empty", array)
            }
          } else {
            (statusText, JsArray.empty)
          }
        }
      }

      val walk = maybeStatusJson.flatMap {
        resp =>
          resp match {
            case ("OK", a: JsArray) => {
              logger.info(s"Response is ok")
              val nextm = a.value.map { p =>
                val res = p.as[String]
                res
              }
              callGraphite(writer, nextm, acc).fallbackTo{
                  callGraphite(writer, nextm, acc)
              }
            }
            case ("Empty", _) => {
              //checkMetric(m).fallbackTo{
              //  checkMetric(m)
              //}.map{ value =>
              //  writer.println(s"$m;$value")
              //}
              writer.println(s"$m")
              Future.successful(acc :+ m)
            }
            case (_, _) => {
              logger.info(s"Error")
              writer.print(m)
              Future.successful(acc)
            }
          }
      }
      walk
    }
    Future.sequence(res).map{
      f => f.flatten
    }
  }

  def checkMetric(metric:String): Future[String] = {
    logger.info(s"check Metric : $metric")
    val mayBeResp = wsClient.url(urlRender).withMethod("POST")
      .withRequestTimeout(Duration(100, TimeUnit.SECONDS))
      .addHttpHeaders("Cookie" -> cookie)
      .addHttpHeaders("Authorization" -> "Basic Zmd1YXk6aWVqMTJMZWV4")
      .addHttpHeaders("Content-Type" -> "application/x-www-form-urlencoded")
      .post(s"target=$metric&from=-1h&until=now&format=json&maxDataPoints=1391")

    logger.info(s"Call value for Metric : $urlRender body : target=$metric&from=-6h&until=now&format=json&maxDataPoints=1391")
    val res = mayBeResp.map{
      resp =>
        val body = resp.body[String]
        logger.info(s"check Metric => response :  $body")
        val jsonBody = Json.parse(body).asOpt[JsArray].getOrElse(JsArray.empty)
        val dataBoints = ((jsonBody.value(0) \ "datapoints").asOpt[JsArray].getOrElse(JsArray.empty))
        dataBoints.value.map { data =>
          val point = data.asOpt[JsArray].getOrElse(JsArray.empty)
          point.value(0).toString()
        }
    }
    val filtered = res.map { values => {
      val dist = values.distinct
      dist.filter(v => !v.equals("null"))
    }}
    val finalres = filtered.map{
      seq => if(seq.isEmpty) "empty" else seq(0)
    }
    finalres
  }
}
