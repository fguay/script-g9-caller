import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import play.api.libs.ws.DefaultBodyReadables._
import play.api.libs.ws.DefaultBodyWritables._

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.io.{Source, StdIn}
import scala.util.Random
import scala.xml.{Attribute, Elem, Node, NodeSeq, Null, Text, XML}
import java.util.UUID.randomUUID

object GluttonSplitter extends App  {

  val splitter:GluttonSplitter = new GluttonSplitter()
  splitter.split()
}


class GluttonSplitter {

  import XMLTools._

  val dataFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:MM:ssXXX") //2020-01-19T23:59:00+01:00

  def split(): Unit ={

    Console.println("Start from file :")
    val filename = StdIn.readLine();
    //val filename = "/Users/fguay/Downloads/glutton2607/canalplusvod_2019-07-26_11-53-19.xml"
    Console.println("Output directory :")
    val outputPath = StdIn.readLine();
    //val outputPath = "/Users/fguay/Downloads/glutton2607/split"

    val xmlFile = XML.load(filename)
    // Filtrage des unit, car on groupera les brands et seasons associées dans le meme fichier.
    val assetsNodes = (xmlFile \ "asset").filter( at => at \@ "objectType" == "unit")
    assetsNodes.foreach{ n => {
      val idKey = getAssetIdKey(n)
      val moved = updateBroadcastDate(n)
      val assetInjected = injectNode(moved)
      takeParent(xmlFile, assetInjected) match {
        case Some(nodes) => writeNode(s"$outputPath/$idKey.xml", nodes)
        case None => Console.println(s"Brand or Season referred in ${idKey} is not present in xml file, ignore unit...")
      }
    }}
  }

  // Changement des dates de diffusion
  def updateBroadcastDate(asset:Node): Node = {
    val maybeBroadcasts = asset \ "broadcasts"
    if(!maybeBroadcasts.isEmpty) {
      val movedBroadcasts = (maybeBroadcasts \ "broadcast").map { b =>
        val startDateNode = b \ "startDate"
        val startDate = dataFormatter.parse(startDateNode.head.text)

        startDate match {
          case d if d.before(new Date()) => replaceChildByLabel(b, "startDate", <startDate>{nextFutureDate(startDate)}</startDate>)
          case _ => b
        }
      }
      replaceChildByLabel(asset, "broadcasts", <broadcasts>{movedBroadcasts}</broadcasts>)
    } else {
      asset
    }
  }

  // on garde la date du jour, hh, mm, ss de la diff mais on change année et mois.
  def nextFutureDate(date:Date) = {
    val cal = Calendar.getInstance()
    val currentYear = cal.get(Calendar.YEAR)
    val currentMonth = cal.get(Calendar.MONTH)
    cal.setTime(date)
    cal.set(Calendar.YEAR, currentYear)
    cal.set(Calendar.MONTH, currentMonth)
    cal.add(Calendar.MONTH, 1)
    Text(dataFormatter.format(cal.getTime))
  }

  // Injection de droits si aucun droit défini
  def injectAndCheckRights(asset:Node): Node = {
    val maybeRights = asset \ "rights"
    maybeRights match {
      case r if r.isEmpty => addNode(asset, fakeRights)
      case _ => asset
    }
  }

  // Injection de medias si aucun medias défini
  def injectAndCheckMedias(asset:Node): Node = {
    val maybeMedias = asset \ "medias"

    maybeMedias match {
      case m if m.isEmpty => addNode(asset, fakeMedia)
      case _ => asset
    }
  }

  // Génération de faux droits avec des dates valident : -2 mois / +4 mois
  def fakeRights: Node = {
    val now = Calendar.getInstance()
    now.roll(Calendar.SECOND, Random.nextInt)
    now.roll(Calendar.MONTH, 2)
    val start = Text(dataFormatter.format(now.getTime))
    now.add(Calendar.MONTH, 4)
    val end = Text( dataFormatter.format(now.getTime))

    val fakeRight =
      <right country="PL" distTech="STREAM"  quality="SD"  subOffer="NCP_CATCHUP" target="ALL">
        <parentalRating authority="KRRIT">3</parentalRating>
        <distributorId>canalplusvod</distributorId>
        <distributorName>CANAL+ Platinum</distributorName>
      </right>

    val rights = addAttributeToNode(addAttributeToNode(fakeRight,Attribute(None, "startDate", start, Null)), Attribute(None, "endDate", end, Null))
    <rights>{rights}</rights>
  }

  // Génération de faux médias (id de workflow auto généré)
  def fakeMedia: Node = {
    val media1 = addAttributeToNode(<transcoding><target>NCP_WEB</target><profile>NCPLUS_HLS_HD</profile></transcoding>, Attribute(None, "idKey", Text(randomUUID().toString), Null))
    val media2 = addAttributeToNode(<transcoding><target>NCP_WEB</target><profile>NCPLUS_DASH_MKPC_HD</profile></transcoding>, Attribute(None, "idKey", Text(randomUUID().toString), Null))
    val media3 = addAttributeToNode(<transcoding><target>NCP_WEB</target><profile>NCPLUS_HSS_HD</profile></transcoding>, Attribute(None, "idKey", Text(randomUUID().toString), Null))

    <medias>
      <media type="video" provider="NCPLUS">
        {media1}
        <quality>HD</quality>
        <bitrate>2000000</bitrate>
        <distPlatform>bifrost</distPlatform>
        <size>40463804</size>
        <qad>false</qad>
        <dst>false</dst>
        <duration>PT2H26M26S</duration>
        <distribId>NCPCDN001</distribId>
        <drmId>SER_21020067.f</drmId>
        <audio>DOLBY_DIGITAL</audio>
        <version>VM</version>
        <provisioning>ready</provisioning>
        <languages original="en">
          <audio>
            <language>pl</language>
            <language>en</language>
          </audio>
          <subtitle>
            <language>pl</language>
          </subtitle>
        </languages>
        <files>
          <file>
            <type>video</type>
            <mimeType>application/x-mpegURL</mimeType>
            <distribURL>https://r.cdn-ncplus.pl/vod/store01/SER_21020067/_/ncplusgo24-vodhlsdrm01.m3u8</distribURL>
          </file>
        </files>
      </media>
      <media type="video" provider="NCPLUS">
        {media2}
        <quality>HD</quality>
        <bitrate>2000000</bitrate>
        <distPlatform>bifrost</distPlatform>
        <size>40463804</size>
        <qad>false</qad>
        <dst>false</dst>
        <duration>PT2H26M26S</duration>
        <distribId>NCPCDN001</distribId>
        <drmId>SER_21020067.c</drmId>
        <audio>DOLBY_DIGITAL</audio>
        <version>VM</version>
        <provisioning>ready</provisioning>
        <languages original="en">
          <audio>
            <language>pl</language>
            <language>en</language>
          </audio>
          <subtitle>
            <language>pl</language>
          </subtitle>
        </languages>
        <files>
          <file>
            <type>video</type>
            <mimeType>application/dash+xml</mimeType>
            <distribURL>https://r.cdn-ncplus.pl/vod/store01/SER_21020067/_/ncplusgo24-voddashdrm01.mpd</distribURL>
          </file>
        </files>
      </media>
      <media type="video" provider="NCPLUS">
        {media3}
        <quality>HD</quality>
        <bitrate>2000000</bitrate>
        <distPlatform>bifrost</distPlatform>
        <size>40463804</size>
        <qad>false</qad>
        <dst>false</dst>
        <duration>PT2H26M26S</duration>
        <distribId>NCPCDN001</distribId>
        <drmId>SER_21020067.c</drmId>
        <audio>DOLBY_DIGITAL</audio>
        <version>VM</version>
        <provisioning>ready</provisioning>
        <languages original="en">
          <audio>
            <language>pl</language>
            <language>en</language>
          </audio>
          <subtitle>
            <language>pl</language>
          </subtitle>
        </languages>
        <files>
          <file>
            <type>video</type>
            <mimeType>application/x-ms-manifest</mimeType>
            <distribURL>https://r.cdn-ncplus.pl/vod/store01/SER_21020067/_/ncplusgo24-vodhssdrm01.ism</distribURL>
          </file>
        </files>
      </media>
    </medias>
  }


  def injectNode(asset:Node) : Node = injectAndCheckRights(injectAndCheckMedias(asset))


  def getAssetIdKey(asset:Node): String = {
    asset.attribute("idKey").getOrElse(asset.attribute("assetId").get).head.text
  }


  def writeNode(output:String, asset:Seq[Node]) = {
    val assets = <assets providerName="CANALPLUSFULL" provider="ncplus" realm="ncplus" xsi:noNamespaceSchemaLocation="assets-v1.6.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">{asset}</assets>
    XML.save(output , assets, "UTF-8", true, null)

  }

  // récupération des parents référencé dans le unit
  def takeParent(doc:Node, asset:Node): Option[Seq[Node]] = {

    val idKey = getAssetIdKey(asset)
    Console.println(s"Split unit :  ${idKey}")

    val mayBeParents = asset \ "edito" \\ "asset"
    if(mayBeParents.isEmpty){
      Some(Seq(asset))
    } else {
      val maybeParentsNode = mayBeParents.map( m => {(m.attribute("idKey").get.head.text, m.attribute("objectType").get.head.text)}).map {
        t => {
          val res = doc \ "asset" find( a => ( a \@ "idKey" == t._1 && a \@ "objectType" == t._2))
          res match {
            case Some(n) => Console.println(s"Found parent ${t._1} - ${t._2}, associated to unit ${idKey}")
            case None => Console.println(s"Parent ${t._1} - ${t._2} not found in xml, but associated to unit ${idKey}")
          }
          res
        }
      }
      if(mayBeParents.flatten.size == maybeParentsNode.flatten.size){
        Some(asset +: maybeParentsNode.flatten)
      } else {
        None
      }
    }

  }
}

// Bidouille pour manipuler les noeuds xml
object XMLTools {

  def addNode(to: Node, newNode: Node) = {
    to match {
      case Elem(prefix, label, attributes, scope, child@_*) => Elem(prefix, label, attributes, scope, child ++ newNode: _*)
      case _ => to
    }
  }

  def deleteNodes(node: Node, f: (Node) => Boolean) = {
    node match {
      case n: Elem => {
        val children = n.child.foldLeft(NodeSeq.Empty)((acc, elem) => if (f(elem)) acc else acc ++ elem)
        n.copy(child = children)
      }
    }
  }

  def replaceChildByLabel(parent: Node, value: String, child: Node) = replaceChild(parent, (elem) => elem.label == value, child)

  def replaceChild(parent: Node, f: (Node) => Boolean, child: Node) = {
    parent match {
      case n: Elem => {
        val children = n.child.foldLeft(NodeSeq.Empty)((acc, elem) => if (f(elem)) acc ++ child else acc ++ elem)
        n.copy(child = children)
      }
    }
  }

  def deleteNodesWithLabel(n: Node, value: String): Elem = deleteNodes(n, (elem) => elem.label == value)

  def addAttributeToNode(node: Node, attribute: Attribute) = {
    node match {
      case n: Elem => n % attribute
    }
  }
}
