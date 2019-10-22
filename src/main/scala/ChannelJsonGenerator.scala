import java.io.{File, PrintWriter}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import LocalCode.{LocalCode, Localized}
import RegionCode.Regionalized
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsArray, Json, OFormat}

import scala.io.{Source, StdIn}

//2019-10-01T00:00:00.000+0100
object ChannelJsonGenerator extends App{
  val gen = new ChannelJsonGenerator()
  gen.run()
}


trait UnifyData  {
  val id: String
}

trait UnifyResourceRef {
  def id: String
}

object LocalCode {
  type LocalCode = String
  type Localized[T] = Map[LocalCode, T]
  val fr = "fr_FR"
  val pl = "pl_PL"
  val en = "en_GB"
  val default: LocalCode = fr
}

object RegionCode {
  type RegionCode = String
  type Regionalized[T] = Map[RegionCode, T]
  val fr = "FR"
  val pl = "PL"
  val en = "EN"
  val default: RegionCode = fr
}

object TagType {
  type TagType = String
  type Tags[T] = Map[TagType, T]
  val genre = "GENRE"
  val default: TagType = genre
}

case class UnifyChannelTvPack(id: String)

case class UnifyTagRef(id: String) extends UnifyResourceRef

case class UnifyPicture(
                         height: Int,
                         width: Int,
                         mimeType: Option[String],
                         url: String,
                         format: String,
                         enabled: Boolean,
                         idKey: String,
                         quality: Option[String],
                         rank: Option[Int] = None,
                         name: Option[String] = None,
                         size: Option[Long] = None,
                         checksum: Option[String] = None,
                         updateDate: Option[ZonedDateTime] = None)

case class ChannelTechResource(
                                id: String,
                                name: String,
                                subscriptionId: String,
                                subOffers: Set[String],
                                geozones: Set[String],
                                target: Map[String, Set[String]])

case class UnifyParentalRating(authority: String, value: String)

case class UnifyChannelLocalization(name: String, shortName: String, baseline: String, subscriptionReason: String)

case class UnifyChannelRegionalization(parentalRatings: Seq[UnifyParentalRating], searchable: Boolean)

case class UnifyChannelTheme(id: String, name: String)

case class UnifyChannelData(
                             id: String,
                             contentType: String,
                             objectType: String = "channel",
                             global: UnifyChannelGlobal,
                             localized: Localized[UnifyChannelLocalization],
                             regionalized: Regionalized[UnifyChannelRegionalization]) extends UnifyData

case class UnifyChannelGlobal(quality: String,
                              beginDate: ZonedDateTime,
                              endDate: ZonedDateTime,
                              creationDate: ZonedDateTime,
                              updateDate: ZonedDateTime,
                              tvPack: UnifyChannelTvPack,
                              tags: Seq[UnifyTagRef],
                              pictures: Seq[UnifyPicture])



case class EditoChannel(val id:String, objectType:String,contentType:String, tags:String, pictures:String, name:String, shortName:String, baseLine:String, subscriptionReason:String, parentalRatingvalue:String, parentalRatingauthority:String, searchable:String)
case class TechChannel(val id:String, val name:String, val subscriptionId:String, val quality:String, val beginDate:String, val endDate:String, val status:String, val tvPackid:String, val subOffers:String, val geozone:String, val targetmycanal:String)

class ChannelJsonGenerator {

  val logger = LoggerFactory.getLogger(classOf[ChannelJsonGenerator])
  private val fullDate = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSZ")

  implicit val unifyPictureFormat: OFormat[UnifyPicture] = Json.format[UnifyPicture]
  implicit val unifyTagRefFormat: OFormat[UnifyTagRef] = Json.format[UnifyTagRef]
  implicit val unifyChannelThemeFormat: OFormat[UnifyChannelTheme] = Json.format[UnifyChannelTheme]
  implicit val unifyChannelTvPackFormat: OFormat[UnifyChannelTvPack] = Json.format[UnifyChannelTvPack]
  implicit val channelTechFormat: OFormat[ChannelTechResource] = Json.format[ChannelTechResource]
  implicit val unifyParentalRatingFormat: OFormat[UnifyParentalRating] = Json.format[UnifyParentalRating]
  implicit val unifyChannelLocalizationFormat: OFormat[UnifyChannelLocalization] = Json.format[UnifyChannelLocalization]
  implicit val unifyChannelGlobalFormat: OFormat[UnifyChannelGlobal] = Json.format[UnifyChannelGlobal]
  implicit val unifyChannelRegionalizationFormat: OFormat[UnifyChannelRegionalization] = Json.format[UnifyChannelRegionalization]
  implicit val unifyChannelDataFormat: OFormat[UnifyChannelData] = Json.format[UnifyChannelData]

  def run(): Unit = {
    println("CSV Edito File : " )
    val editoFile = "/Users/fguay/Downloads/csv_unify_channel.csv" //StdIn.readLine()
    println("CSV Tech File : " )
    val techFile = "/Users/fguay/Downloads/csv_hour_channel.csv" //StdIn.readLine()
    println("Output json Directory : " )
    val output = "/Users/fguay/Downloads/json" //StdIn.readLine()
    //Console.println("Unify URL :")
    //val unifyUrl = StdIn.readLine()

    val editoChannels = Source.fromFile(editoFile).getLines.toList.map{
        line => {
          line.split(";") match {
            case Array(id,objectType,contentType, tags, pictures, name, shortName, baseLine, subscriptionReason, parentalRatingvalue, parentalRatingauthority, searchable)
            => EditoChannel(id,objectType,contentType, tags, pictures, name, shortName, baseLine, subscriptionReason, parentalRatingvalue, parentalRatingauthority, searchable)
          }
        }
    }

    val techChannels = Source.fromFile(techFile).getLines.toList.map{
      line => {
        line.split(";") match {
          case Array(id, name, subscriptionId, quality, beginDate, endDate, status, tvPackid, subOffers, geozone, targetmycanal)
          => TechChannel(id, name, subscriptionId, quality, beginDate, endDate, status, tvPackid, subOffers, geozone, targetmycanal)
        }
      }
    }

    val channels = editoChannels.sortBy(_.id).zip(techChannels.sortBy(_.id)).map{
      case (edito, tech) if(edito.id.equals(tech.id)) => {
        logger.info(s"edito ${edito.id} - ${tech.id}")
        val unify = UnifyChannelData(
          edito.id,
          edito.contentType,
          edito.objectType,
          UnifyChannelGlobal(
            tech.quality,
            ZonedDateTime.from(fullDate.parse(tech.beginDate)),
            ZonedDateTime.from(fullDate.parse(tech.endDate)),
            ZonedDateTime.from(fullDate.parse("2019-10-01T00:00:00.000+0100")),
            ZonedDateTime.from(fullDate.parse("2019-10-01T00:00:00.000+0100")),
            UnifyChannelTvPack(
              tech.tvPackid
            ),
            Seq[UnifyTagRef](),
            Seq[UnifyPicture]()),
          Map[LocalCode, UnifyChannelLocalization]( LocalCode.pl ->
            UnifyChannelLocalization(
              edito.name,
              edito.shortName,
              edito.baseLine,
              edito.subscriptionReason
            )),
          Map[LocalCode,UnifyChannelRegionalization]( LocalCode.pl ->
            UnifyChannelRegionalization(
              Seq[UnifyParentalRating](
                UnifyParentalRating(
                  edito.parentalRatingauthority,
                  edito.parentalRatingvalue)),
              true
            ))
        )
        val hour = ChannelTechResource(
          tech.id,
          edito.name,
          tech.subscriptionId,
          tech.subOffers.replace("\"", "").split(",").toSet,
          tech.geozone.replace("\"", "").split(",").toSet,
          Map( "mycanal" -> tech.targetmycanal.replace("\"", "").split(",").toSet)
        )
        (unify, hour)
      }
    }

    val (unify, techs) = channels.unzip
    techs.map( tech => {
        val js = Json.toJson(tech)
        val writer = new PrintWriter(new File(s"$output/tech/${tech.id}.json"))
        writer.write(Json.prettyPrint(js))
        writer.close()
      }
    )
    unify.map {
      edito => {
        val js = Json.toJson(edito)
        val writer = new PrintWriter(new File(s"$output/unify/${edito.id}.json" ))
        writer.write(Json.prettyPrint(js))
        writer.close()
      }
    }




  }



}
