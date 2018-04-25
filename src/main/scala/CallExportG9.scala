import java.io.{File, PrintWriter}

import org.slf4j.LoggerFactory
import play.api.libs.json.{JsArray, JsString, JsValue}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.{Source, StdIn}


object CallExportG9 extends App  {
  val t = new CallExportG9()
  t.callExport()
}

class CallExportG9  {
  import scala.concurrent.ExecutionContext.Implicits._
  val logger = LoggerFactory.getLogger(classOf[CallExportG9])

  def callExport(): Unit ={



    val writerBroadcast = new PrintWriter(new File("/tmp/broadcast.txt"))
    val writerEdito = new PrintWriter(new File("/tmp/edito.txt"))
    val writerBrand = new PrintWriter(new File("/tmp/brand.txt"))
    val writerSeason = new PrintWriter(new File("/tmp/season.txt"))
    val writerCluster = new PrintWriter(new File("/tmp/cluster.txt"))

    Console.println("Blue or grean [B/G]:")
    val bg = StdIn.readLine() match{ case "B" => "?bg=blue" case _ => "" }

    logger.info("start pooling export...")
    val b = recCallExport("https://export.canal-plus.io/latest/gbox/broadcast", bg, writerBroadcast, transformBraodcast).map{_ => writerBroadcast.close()}
    var e = recCallExport("https://export.canal-plus.io/latest/gbox/edito/unit", bg, writerEdito, transformEdito).map{_ => writerEdito.close()}
    //var s = recCallExport("https://export.canal-plus.io/latest/gbox/edito/brand", bg, writerBrand, transformBrand).map{_ => writerBrand.close()}
    //var ss = recCallExport("https://export.canal-plus.io/latest/gbox/edito/season", bg, writerSeason, transformSeason).map{_ => writerSeason.close()}

    val zipped = e.zip(b)//.zip(s).zip(ss)
    zipped.onFailure{
      case t => logger.error(t.getMessage,t)
    }
    zipped.onComplete{
      _ => {
        WSClient.close()
      }
    }
  }

  def logSpecific(js: JsValue, exportType: String): Unit = {
    val channelId = (js \ "channel" \ "id").as[String]

    if(channelId == "301") {
      println(s"$exportType - $js")
    }
  }


  def transformSeason(jsArray: JsArray):Seq[String]= {
    jsArray.value.map{
      b => {
        ((b \ "id").as[String]  + ";"  + (b \ "brandId").getOrElse(JsString("")).as[String] + ";" + (b \ "channel" \ "id").getOrElse(JsString("")).as[String] + "\n")

      }
    }
  }

  def transformBrand(jsArray: JsArray):Seq[String]= {
    jsArray.value.map{
      b => {
        ((b \ "id").as[String] + ";" + (b \ "channel" \ "id").getOrElse(JsString("")).as[String] + "\n")
      }
    }
  }

  def transformBraodcast(jsArray: JsArray):Seq[String]= {
    jsArray.value.map{
      b => {
        ((b \ "id").as[String] + ";" + (b \ "editoId").getOrElse(JsString("")).as[String] + ";" + (b \ "channel" \ "id").getOrElse(JsString("NOCHANNEL")).as[String] + ";" + (b \ "availability" \ "startDate").as[String] + ";" + (b \ "availability" \ "endDate").as[String]+ "\n")
      }
    }
  }

  def transformEdito(jsArray: JsArray):Seq[String]= {
    jsArray.value.map{
      b => {
        ((b \ "id").as[String] + ";" + (b \ "seasonId").getOrElse(JsString("")).as[String] + ";"  + (b \ "brandId").getOrElse(JsString("NOCHANNEL")).as[String] + ";" + (b \ "channel" \ "id").getOrElse(JsString("NULL")).as[String] + "\n")
      }
    }
  }



  def recCallExport(url: String, bg: String, writer: PrintWriter, trans: JsArray => Seq[String]) : Future[String] = {
    logger.info("call next URL : " + url + bg)
    WSClient.callExport(url + bg).flatMap{
        response => {
         response._1 match {
           case "OK" => {
             trans((response._2 \ "data").as[JsArray]).map{ data =>
               writer.write(data)
             }
             (response._2 \ "paging" \ "next").toOption match {
               case Some(url) => recCallExport(url.as[String], bg, writer, trans);
               case None => Future("Done")
             }
           }
           case _ =>  {
             logger.error("response -> " + response._1 )
             recCallExport(url, bg, writer, trans)
           }
         }
        }
      }
  }


}
