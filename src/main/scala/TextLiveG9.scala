import java.io.{File, PrintWriter}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.io.{Source, StdIn}
import scala.util._
import play.libs.ws

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._


object CompleteInfoG9 extends App  {


  val t = new CompleteInfoG9()
  t.callContent()

}
class CompleteInfoG9  {

  val logger = LoggerFactory.getLogger(classOf[CompleteInfoG9])
  import scala.concurrent.ExecutionContext.Implicits._

  def callContent(): Unit ={
    val filename = "/tmp/result-analyse.csv"
    println("read file : " + filename)
    Console.println("Output file:")
    val outputfilename = StdIn.readLine()
    println("write file : " + outputfilename)
    Console.println("Host catalog:")
    val bghapi = StdIn.readLine()
    Console.println("Host hour:")
    val bghour = StdIn.readLine()
    Console.println("Host ES:")
    val bges = StdIn.readLine()
    Console.println("Host Dataservice:")
    val bddata = StdIn.readLine()

    val writer = new PrintWriter(new File(outputfilename))
    writer.write("idEdito;idBroadcast;In Catalog Vod80;NbDoc Vod80;Interval Vod80;idKey VOD_80;SubOffer Vod80;NbDoc Broadcast;Channel Broadcast;ContentId BroadCast;Interval Broadcast;Suboffer Broadcast;Edito Terrapi;Identity Terrapi; Hour Tech; Size Queue Tech; Size Queue Broadcast; Size Queue Identity; Size Queue Edito;Status BackEnd;Status Queue;PLM Status\n")


    Source.fromFile(filename).getLines.toList.map(
      line => {
        val ids = line.split(",")

        val futureContent = WSClient.callContent(bghapi, ids(0)).map{
          res => (res._1, res._2)
        }
        val futureES = WSClient.callESByEdito(bges, "xwing-vod_80",ids(0).split("_")(0)).map{
          res => (res._1, res._2)
        }
        val futureDiff = WSClient.callESByEdito(bges, "xwing-broadcast",ids(0).split("_")(0)).map{
          res => (res._1, res._2)
        }
        val futureEditoTerr = WSClient.callTerapiEdito(ids(0).split("_")(0)).map{
          res => (res._1, res._2)
        }
        val futureHour = WSClient.callHour(bghour, ids(1)).map{
          res => (res._1, res._2)
        }
        val futurQueueHourTech = WSClient.callQueue(bddata, s"hour:tech:${ids(1)}").map{
          res => (res._1, res._2)
        }
        val futurQueueHourDiff = WSClient.callQueue(bddata, s"hour:broadcast:${ids(1)}").map{
          res => (res._1, res._2)
        }
        val futurQueueIdentity = WSClient.callQueue(bddata, s"bddp:identities:contents:${ids(0).split("_")(0)}").map{
          res => (res._1, res._2)
        }
        val futurQueueEdito = WSClient.callQueue(bddata, s"bddp:contents:product:${ids(0).split("_")(0)}").map{
          res => (res._1, res._2)
        }
        val futureIntTerr = WSClient.callTerapiIdentity(ids(1).split("_")(1)).map{
          res => (res._1, res._2)
        }

        val res = Future.sequence(Seq(futureContent, futureES, futureDiff, futureEditoTerr, futureHour, futureIntTerr,futurQueueHourTech, futurQueueHourDiff, futurQueueIdentity, futurQueueEdito)).map{
          case Seq(hasContent, hasEs, hasDiff, hasEditoTerr, hasHour, hasIdentTerr, hasQueueTech, hasQueueDiff, hasQueueIdentity, hasQueueEdito) => {
            val nbRes = (hasEs._2 \ "hits" \ "total").getOrElse(JsNumber(0))
            val interval = (hasEs._2 \ "hits" \ "hits").getOrElse(JsArray.empty).as[JsArray].value.map{
              v => (v \ "_source" \ "intervalKey").asOpt[JsString].getOrElse(JsString("")).value
            }
            val plmEditos = (hasEs._2 \ "hits" \ "hits").getOrElse(JsArray.empty).as[JsArray].value.map{
              v => (v \ "_source" \ "idKey").asOpt[JsString].getOrElse(JsString("")).value
            }
            val suboffers = (hasEs._2 \ "hits" \ "hits").getOrElse(JsArray.empty).as[JsArray].value.map{
              v => (v \ "_source" \ "subOffers").asOpt[JsArray].getOrElse(JsArray.empty).value.mkString(",")
            }
            val intervalDiff = (hasDiff._2 \ "hits" \ "hits").getOrElse(JsArray.empty).as[JsArray].value.map{
              v => (v \ "_source" \ "startDate").asOpt[JsString].getOrElse(JsString("")).value + "_" + (v \ "_source" \ "endDate").asOpt[JsString].getOrElse(JsString("")).value
            }
            val suboffersDiff = (hasDiff._2 \ "hits" \ "hits").getOrElse(JsArray.empty).as[JsArray].value.map{
              v => {
                (v \ "_source" \ "subOffers").asOpt[JsArray].getOrElse(JsArray.empty).value.mkString(",")
              }
            }
            val contentId = (hasDiff._2 \ "hits" \ "hits").getOrElse(JsArray.empty).as[JsArray].value.map{
              v => {
                (v \ "_source" \ "contentId").asOpt[JsString].getOrElse(JsString("")).value
              }
            }
            val nbResBroadcast = (hasDiff._2 \ "hits" \ "total").getOrElse(JsNumber(0))

            val channelDiff = (hasDiff._2 \ "hits" \ "hits").getOrElse(JsArray.empty).as[JsArray].value.map{
              v => {
                (v \ "_source" \ "channel").asOpt[JsString].getOrElse(JsString("")).value
              }
            }

            val queueTech = (hasQueueTech._2).asOpt[JsArray].getOrElse(JsArray.empty).as[JsArray].value.size
            val queueBroadcast = (hasQueueDiff._2).asOpt[JsArray].getOrElse(JsArray.empty).as[JsArray].value.size
            val queueIdentity = (hasQueueIdentity._2).asOpt[JsArray].getOrElse(JsArray.empty).as[JsArray].value.size
            val queueEdito = (hasQueueEdito._2).asOpt[JsArray].getOrElse(JsArray.empty).as[JsArray].value.size

            val queuestatus = if (queueTech + queueBroadcast + queueIdentity + queueEdito == 4) "OK" else "KO"
            val backendstatus = if(s"${hasEditoTerr._1}${hasIdentTerr._1}${hasHour._1}".equals("OKOKOK")) "OK" else "KO"
            val plmstatus = if(plmEditos.contains(ids(1))) "OK" else "KO"

            logger.info(ids(0)+";" + ids(1) +";"+hasContent._1+";" +nbRes.as[BigDecimal]+ ";" + interval.distinct.mkString(",") + ";" +  plmEditos.distinct.mkString(",") + ";" + suboffers.distinct.mkString(",") + ";" + nbResBroadcast.as[BigDecimal] + ";" + channelDiff.distinct.mkString(",") +  ";" + contentId.distinct.mkString(",") + ";"  + intervalDiff.distinct.mkString(",") +  ";" + suboffersDiff.distinct.mkString(",") + ";" + hasEditoTerr._1 + ";" + hasIdentTerr._1 + ";" + hasHour._1 + ";" + queueTech + ";" + queueBroadcast + ";" + queueIdentity + ";" + queueEdito +";" + queuestatus + ";" + backendstatus + ";" + plmstatus)
            writer.write(ids(0)+";" + ids(1) +";"+hasContent._1+";" +nbRes.as[BigDecimal]+ ";" + interval.distinct.mkString(",") + ";" +  plmEditos.distinct.mkString(",") +  ";" + suboffers.distinct.mkString(",") + ";" + nbResBroadcast.as[BigDecimal] + ";" + channelDiff.distinct.mkString(",") + ";" + contentId.distinct.mkString(",") + ";"  + intervalDiff.distinct.mkString(",") +  ";" + suboffersDiff.distinct.mkString(",") + ";" + hasEditoTerr._1 + ";" + hasIdentTerr._1 + ";"  + hasHour._1 + ";" + queueTech + ";" + queueBroadcast + ";" + queueIdentity + ";" + queueEdito +";" + queuestatus + ";" + backendstatus +  ";" + plmstatus + "\n")
          }
        }
        Await.result(res, 120 seconds)
      }
    )

    WSClient.close()
    writer.close()
  }


}
