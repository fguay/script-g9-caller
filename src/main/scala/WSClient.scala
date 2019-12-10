import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.libs.json.{JsObject, JsString, JsValue, Json}
import play.api.libs.ws._
import play.api.libs.ws.ahc._
import play.api.libs.json._
import play.api.libs.ws.JsonBodyReadables._
import play.api.libs.ws.JsonBodyWritables._

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.parsing.json.JSONObject


object WSClient {

  implicit val system = ActorSystem()
  system.registerOnTermination {
    System.exit(0)
  }
  implicit val materializer = ActorMaterializer()
  import DefaultBodyReadables._
  import scala.concurrent.ExecutionContext.Implicits._

  val wsClient = StandaloneAhcWSClient()


  def callQueue(host:String, key:String): Future[(String,JsValue)] = {
    call("GET",s"http://$host/admin/redis/default/?showValue=true&keyPattern=$key", None, None, ("Content-Type"->"application/json"))
  }

  def callExport(url:String): Future[(String,JsValue)] ={
    call("GET", url,
      Some(("g9prod","ODVmNTc1ZDc5ZTYwZjRiNGJhOGVjMTdl")), None,
       ("Content-Type"->"application/json"))
  }

  def callContent(host:String, id:String): Future[(String,JsValue)] ={
    call("GET", "http://"+host+"/catalog/unit/" + id,
      None, None, ("Content-Type"->"application/json"), ("xx-service"->"canalreplay"), ("xx-operator"->"g9"), ("xx-domain"->"json"), ("xx-suboffers"->"CP_ALD, CP_ALD_X, ") )
  }

  def callBroadcast(host:String, id:String): Future[(String, JsValue)] ={
    call("GET", "http://"+host+"/live/broadcast/" + id,
      None, None, ("Content-Type"->"application/json"), ("xx-service"->"canalreplay"), ("xx-operator"->"g9"), ("xx-domain"->"json"), ("xx-suboffers"->"CP_ALD, CP_ALD_X") )
  }

  def callHour(host:String, id:String): Future[(String, JsValue)] ={
    call("GET", "http://"+host+"/timeline/content/"+id,
      None, None, ("Content-Type"->"application/json"))
  }

  def callTerapiIdentity(id:String): Future[(String, JsValue)] ={
    call("GET", "http://172.31.88.94:8080/api/identity/external/broadcast/PLM/"+id+"?Access-Token=11BBDA2443A75BE6B7329CC6B164A",
      None, None, ("Content-Type"->"application/json") , ("Accept-Language" -> "fr"))
  }

  def callTerapiEdito(id:String): Future[(String, JsValue)] ={
    call("GET", "http://172.31.88.94:8080/api/contents/"+id+"?Access-Token=11BBDA2443A75BE6B7329CC6B164A",
      None, None, ("Content-Type"->"application/json"), ("Accept-Language" -> "fr"))
  }

  def callESByEdito(host:String, index:String, idEdito:String): Future[(String, JsValue)] = {
    call("POST",
      "http://"+host+"/" + index + "/_search",
      None,
      Some(Json.parse(s"""{"query": {"bool": {"must": [{"wildcard": {"idEdito": "$idEdito*"}}],"must_not": [],"should": []}},"from": 0,"size": 700,"sort": [],"aggs": {},"version": true}""").as[JsObject]),
      ("Content-Type"->"application/json"))
  }

  def call(method:String, url:String, auth: Option[(String,String)], body:Option[JsObject], headers: (String, String)*) : Future[(String, JsValue)] = {

    val req = auth match {
        case Some((login,pass))  => wsClient.url(url).withRequestTimeout(Duration(15, TimeUnit.SECONDS)).withAuth(login, pass,WSAuthScheme.BASIC)
        case _ => wsClient.url(url)
    }



    val resp = method match {
      case "PUT" => req.withHttpHeaders(headers: _*).put(body.getOrElse(JsObject.empty))
      case "POST" => req.withHttpHeaders(headers: _*).post(body.getOrElse(JsObject.empty))
      case _ => req.withHttpHeaders(headers: _*).get()
    }

    resp.map { response ⇒
      val statusText: String = response.statusText
      val body = response.body[String]
      if(statusText.equals("OK")){
        (statusText, Json.parse(body))
      } else {
        (statusText, JsObject.empty)
      }
    }
  }

  def close(): Unit ={
    wsClient.close()
    system.terminate()
  }


}
