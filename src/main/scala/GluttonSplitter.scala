
import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, NoSuchElementException}

import play.api.libs.ws.DefaultBodyReadables._
import play.api.libs.ws.DefaultBodyWritables._

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.io.{Source, StdIn}
import scala.util.Random
import scala.xml.{Attribute, Elem, Node, NodeSeq, Null, Text, XML}
import java.util.UUID.randomUUID

import org.slf4j.LoggerFactory


object GluttonSplitter extends App  {

  val splitter:GluttonSplitter = new GluttonSplitter()
  splitter.split()
}

case class Provider(providerName:String, provider:String, realm:String )

class GluttonSplitter {

  import XMLTools._
  val logger = LoggerFactory.getLogger(classOf[GluttonSplitter])

  val dataFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:MM:ssXXX") //2020-01-19T23:59:00+01:00

  def split(): Unit ={

    Console.println("From directory :")
    val inputPath = StdIn.readLine();
    //val inputPath = "/Users/fguay/Downloads/glutton"

    Console.println("Output directory :")
    val outputPath = StdIn.readLine();
    //val outputPath = "/Users/fguay/Downloads/split4"

    val outputChild = new File(outputPath + "/unit")
    if(!outputChild.exists()){
      outputChild.mkdirs()
    }

    val outputBrand = new File(outputPath + "/brand")
    if(!outputBrand.exists()){
      outputBrand.mkdirs()
    }

    val outputSeason = new File(outputPath + "/season")
    if(!outputSeason.exists()){
      outputSeason.mkdirs()
    }

    val outputError = new File(outputPath + "/error")
    if(!outputError.exists()){
      outputError.mkdirs()
    }

    val files = (new File(inputPath)).listFiles.filter(_.isFile)

    files.foreach {
      filename => splitFile(filename.getAbsolutePath)
    }

    def splitFile(filename:String): Unit ={
      logger.info(s"Read file : $filename")
      val xmlFile = XML.load(filename)
      // Filtrage des unit, car on groupera les brands et seasons associées dans le meme fichier.

      val provider = getProvider(xmlFile)
      val assetsNodes = (xmlFile \ "asset")
      assetsNodes.foreach{
        n => {
          val idKey = getAssetIdKey(n)
          val objectType = getObjectType(n)
          val mayBeParents = n \ "edito" \\ "asset"
          if("unit".equals(objectType) && mayBeParents.isEmpty){
            writeNode(s"$outputPath/$idKey.xml",provider, n)
          } else {
            writeNode(s"$outputPath/$objectType/$idKey.xml",provider, n)
          }
        }
      }

    }

    def mergeParent(filename:String, outputDir:String): Unit ={
      val xmlFile = XML.load(filename)

      val provider = getProvider(xmlFile)
      val assetNode = (xmlFile \ "asset").head
      val idKey = getAssetIdKey(assetNode)
      try {
        val parentsNode = takeParentFromFile(outputPath, assetNode)
        parentsNode match {
          case Some(nodes) => writeNode(s"$outputDir/$idKey.xml", provider, (assetNode :+ nodes).flatten)
          case None => writeNode(s"$outputPath/error/$idKey.xml", provider, assetNode)
        }
      } catch {
        case e:NoSuchElementException => {
          logger.error(e.getMessage)
          writeNode(s"$outputPath/error/$idKey.xml", provider, assetNode)
        }
      }
    }

    val fileSeason = outputSeason.listFiles().filter(_.isFile)
    fileSeason.foreach{
      filename => {
        mergeParent(filename.getAbsolutePath, s"$outputPath/season")
      }
    }

    val fileChildren = outputChild.listFiles.filter(_.isFile)
    fileChildren.foreach{
      filename => mergeParent(filename.getAbsolutePath, s"$outputPath")
    }

  }



  def getProvider(assets:Node): Provider ={
    Provider(
      assets \@ "providerName",
      assets \@ "provider",
      assets \@ "realm")
  }


  def getAssetIdKey(asset:Node): String = {
    asset.attribute("idKey").getOrElse(asset.attribute("assetId").get).head.text
  }

  def getObjectType(asset:Node): String = {
    asset.attribute("objectType").getOrElse(asset.attribute("objectType").get).head.text
  }


  def writeNode(output:String, provider:Provider, asset:Seq[Node]) = {
    logger.info(s"Write XML : ${output}")
    val assets = <assets xsi:noNamespaceSchemaLocation="assets-v1.6.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">{asset}</assets> % Attribute(None, "providerName", Text(provider.providerName), Null) % Attribute(None, "provider", Text(provider.provider), Null) % Attribute(None, "realm", Text(provider.realm), Null)
    XML.save(output , assets, "UTF-8", true, null)
  }

  def takeParentFromFile(output:String, asset: Node): Option[Seq[Node]] = {
    val mayBeParents = asset \ "edito" \\ "asset"
    if(mayBeParents.isEmpty){
      None
    }  else {
      val maybeParentsNode = mayBeParents.map( m => {(m.attribute("idKey").get.head.text, m.attribute("objectType").get.head.text)}).map {
        t => {
          val seasonFile = new File(s"$output/${t._2}/${t._1}.xml")
          if(seasonFile.exists()) {
            val parentf = XML.loadFile(s"$output/${t._2}/${t._1}.xml")
            (parentf \ "asset").toSeq
          } else {
            //logger.error(s"Parent type : ${t._2} with id ${t._1} not exist !!! ")
            throw new NoSuchElementException(s"Parent type : ${t._2} with id ${t._1} not exist !!! ")
          }
        }
      }
      Some(maybeParentsNode.flatten)
    }
  }

  // récupération des parents référencé dans le unit dans le meme fichier...
  def takeParent(doc:Node, asset:Node): Option[Seq[Node]] = {

    val idKey = getAssetIdKey(asset)
    val mayBeParents = asset \ "edito" \\ "asset"
    if(mayBeParents.isEmpty){
      None
    } else {
      val maybeParentsNode = mayBeParents.map( m => {(m.attribute("idKey").get.head.text, m.attribute("objectType").get.head.text)}).map {
        t => {
          doc \ "asset" find( a => ( a \@ "idKey" == t._1 && a \@ "objectType" == t._2))
        }
      }
        Some(maybeParentsNode.flatten)
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
