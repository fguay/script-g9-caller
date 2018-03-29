import java.io.{File, PrintWriter}
import java.util.concurrent.TimeUnit

import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsArray, JsString, JsValue}

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.io.StdIn


object CallExportG9 extends App  {
  val t = new CallExportG9()
  t.callExport()
}

class CallExportG9  {
  import scala.concurrent.ExecutionContext.Implicits._
  val logger: Logger = LoggerFactory.getLogger(classOf[CallExportG9])

  def callExport(): Unit ={



    val writerBroadcast = new PrintWriter(new File("/tmp/broadcast.txt"))
    val writerEdito = new PrintWriter(new File("/tmp/edito.txt"))
    val writerBrand = new PrintWriter(new File("/tmp/brand.txt"))
    val writerSeason = new PrintWriter(new File("/tmp/season.txt"))
    //val writerCluster = new PrintWriter(new File("/tmp/cluster.txt"))

    Console.println("Blue or grean [B/G]:")
    val bg = StdIn.readLine() match{ case "B" => "?bg=blue" case _ => "" }

    logger.info("start pooling export...")
    val b = recCallExport("https://export.canal-plus.io/latest/gbox/broadcast", bg, writerBroadcast, transformBraodcast).map{_ => writerBroadcast.close()}
    var e = recCallExport("https://export.canal-plus.io/latest/gbox/edito/unit", bg, writerEdito, transformEdito).map{_ => writerEdito.close()}
   // var s = recCallExport("https://export.canal-plus.io/latest/gbox/edito/brand", bg, writerBrand, transformBrand).map{_ => writerBrand.close()}
    //var ss = recCallExport("https://export.canal-plus.io/latest/gbox/edito/season", bg, writerSeason, transformSeason).map{_ => writerSeason.close()}

    val zipped = b.zip(e)
    zipped.onFailure{
      case t => logger.error(t.getMessage,t)
    }
    zipped.onComplete{
      _ => {
        WSClient.close()
      }
    }
  }


  def transformSeason(jsArray: JsArray):Seq[String]= {
    jsArray.value.collect{
      case b if (b \ "channel" \ "id").asOpt[String].isDefined  => {
        logSpecific(b, "SEASON")

        (b \ "id").as[String] + ";" + (b \ "brandId").getOrElse(JsString("")).as[String] + ";" + (b \ "channel" \ "id").as[String] + "\n"
      }
    }

    /*
    jsArray.value.map{
      b => {
        ((b \ "id").as[String]  + ";"  + (b \ "brandId").getOrElse(JsString("")).as[String] + ";" + (b \ "channel" \ "id").as[String] + "\n")

      }
    }
    */
  }

  def logSpecific(js: JsValue, exportType: String): Unit = {
    val channelId = (js \ "channel" \ "id").as[String]

    if(channelId == "301") {
      println(s"$exportType - $js")
    }
  }

  def transformBrand(jsArray: JsArray):Seq[String]= {

    jsArray.value.collect{
      case b if (b \ "channel" \ "id").asOpt[String].isDefined => {
        logSpecific(b, "BRAND")
        (b \ "id").as[String] + ";" + (b \ "channel" \ "id").as[String] + "\n"
      }
    }

    /*
    jsArray.value.map{
      b => {
        ((b \ "id").as[String] + ";" + (b \ "channel" \ "id").as[String] + "\n")
      }
    }
    */
  }

  /**
  def transformBraodcast(jsArray: JsArray):Seq[String]= {
    jsArray.value.collect{
      case b if (b \ "channel" \ "id").asOpt[String].isDefined  => {
        logSpecific(b, "BROADCAST")
        (b \ "id").as[String] + ";" + (b \ "editoId").as[String] + ";" + (b \ "channel" \ "id").as[String] + ";" + (b \ "availability" \ "startDate").as[String] + ";" + (b \ "availability" \ "endDate").as[String] + "\n"
      }
    }
  }
    **/

  def transformBraodcast(jsArray: JsArray):Seq[String]= {
    jsArray.value.map{
      b => {
        logSpecific(b, "BROADCAST")
        ((b \ "id").as[String] + ";" + (b \ "editoId").as[String] + ";" + (b \ "channel" \ "id").as[String] + "\n")
      }
    }
  }

  /**
  def transformEdito(jsArray: JsArray):Seq[String]= {
    jsArray.value.collect{
      case b if (b \ "channel" \ "id").asOpt[String].isDefined  => {
        logSpecific(b, "EDITO")
        (b \ "id").as[String] + ";" + (b \ "seasonId").getOrElse(JsString("")).as[String] + ";" + (b \ "brandId").getOrElse(JsString("")).as[String] + ";" + (b \ "channel" \ "id").getOrElse(JsString("NULL")).as[String] + "\n"
      }
    }
  }
    **/

  def transformEdito(jsArray: JsArray):Seq[String]= {
    jsArray.value.map{
      b => {

        logSpecific(b, "EDITO")

        ((b \ "id").as[String] + ";" + (b \ "seasonId").getOrElse(JsString("")).as[String] + ";"  + (b \ "brandId").getOrElse(JsString("")).as[String] + ";" + (b \ "channel" \ "id").getOrElse(JsString("NULL")).as[String] + "\n")
      }
    }
  }

  def recCallExport(url: String, bg: String, writer: PrintWriter, trans: JsArray => Seq[String]) : Future[String] = {
    logger.info("call next URL : " + url + bg)
    WSClient.callExport(url + bg).flatMap{
        response => {

          val js = (response._2 \ "data").asOpt[JsArray].getOrElse{
            throw new Exception(s"url: ${url + bg} ## response: ${response._2 }")
          }

          trans(js).foreach{ data =>
            writer.write(data)
          }
           (response._2 \ "paging" \ "next").toOption match {
            case Some(nextUrl) => recCallExport(nextUrl.as[String], bg, writer, trans);
            case None => Future("Done")
          }
        }
      }

  }


}
