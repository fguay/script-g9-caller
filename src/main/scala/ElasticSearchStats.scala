import java.io.{File, PrintWriter}

import WSClient.call
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsArray, JsNumber, JsObject, JsString, JsValue}

import scala.concurrent.Future
import scala.io.StdIn

object ElasticSearchStats extends App{
  val es = new ElasticSearchStats();
  es.stats
}

case class EsCluster(val nodes:Seq[EsNode], val indices:Seq[String], route:Seq[EsRoute])
case class EsNode(val id:String, val url:String)
case class EsIndex(val id:String)
case class EsRoute(val indexId:String, val nodesId:Seq[String])
case class EsIndexStats(val indexId:String, val count:Long, val deleted:Long, val search:Long, val indexing:Long, val get:Long)

class ElasticSearchStats  {

  val logger = LoggerFactory.getLogger(classOf[ElasticSearchStats])
  import scala.concurrent.ExecutionContext.Implicits._


  def stats = {

    println("ES Host : " )
    val esHost = StdIn.readLine()
    println("Call  Host : " + esHost)

    Console.println("Output file:")
    val outputfilename = StdIn.readLine()
    println("write file : " + outputfilename)



    val maybeCluster = call("GET",s"http://$esHost:9200/_cluster/state", None, None, ("Content-Type"->"application/json"))
    val maybeStats = call("GET",s"http://$esHost:9200/_stats", None, None, ("Content-Type"->"application/json"))

    val cluster = maybeCluster.map{ tpl =>  parseCluster(tpl) }
    val clusterStats = maybeStats.map{ tpl =>  parseClusterStats(tpl) }

    val writer = new PrintWriter(new File(outputfilename))
    writer.write(s"Index;NbDoc;NbDelete;NbSearch;NbGet;NbIndex;NbShard;Nodes\n")

    val writing = cluster.zip(clusterStats).map{
      case (clusterData, seqIndexStats) => {
        seqIndexStats.foreach{ idx =>
          val nodesUrl = getNodesUrl(idx.indexId, clusterData)
          logger.info(s"${idx.indexId};${idx.count};${idx.deleted};${idx.search};${idx.get};${idx.indexing};${nodesUrl.size};${nodesUrl.mkString(",")}")
          writer.write(s"${idx.indexId};${idx.count};${idx.deleted};${idx.search};${idx.get};${idx.indexing};${nodesUrl.size};${nodesUrl.mkString(",")}\n")
        }
      }
    }

    writing.onComplete {
      _ =>
      writer.close()
      WSClient.close()
    }

    writing.onFailure{
      case t => {
        logger.error(t.getMessage,t)
      }
    }

  }

  def getNodesUrl(indexId:String, cluster:EsCluster) = {
    val nodeIds = cluster.route.filter(r => r.indexId.equals(indexId)).head.nodesId

    nodeIds.map { id =>
      cluster.nodes.filter(n => n.id.equals(id)).head.url
        .replace(".mg.prod.eu-west-1.aws", "")
        .replace("p1-hapi-catalogsearchdata", "")
    }
  }

  def parseCluster(res:(String, JsValue)):EsCluster = {
    val js = res._2
    val nodesJs = (js \ "nodes").as[JsObject]
    val indicesJs = (js \ "metadata" \ "indices").as[JsObject]
    val routeJs = (js \ "routing_table" \ "indices").as[JsObject]

    val indicesKeys = indicesJs.keys;
    val nodesKeys = nodesJs.keys

    val nodes = nodesKeys.map{ idx =>
      EsNode(idx, (nodesJs \ idx \ "name").as[JsString].value)
    }.toSeq
    val indices = indicesJs.keys.toSeq

    val route = indices.map{ idx =>
      val nodes = (routeJs \ idx \ "shards" \ "0").getOrElse(JsArray.empty).as[JsArray].value.map{
        js => (js \ "node").getOrElse(JsString("")).as[JsString].value
      }.toSeq
      EsRoute(idx, nodes)
    }.toSeq
    EsCluster(nodes, indices, route)
  }


  def parseClusterStats(res:(String, JsValue)):Seq[EsIndexStats] = {
    val js = res._2
    val indices = (js \ "indices").as[JsObject]
    val indicesKeys = indices.keys

    val ret = indicesKeys.map { idx =>
      val count = (indices \ idx \ "total" \ "docs" \ "count" ).getOrElse(JsNumber(0)).as[JsNumber].value.toLong
      val deleted = (indices \ idx \ "total" \ "docs" \ "deleted").getOrElse(JsNumber(0)).as[JsNumber].value.toLong
      val search = (indices \ idx \ "total" \ "search" \ "query_total").getOrElse(JsNumber(0)).as[JsNumber].value.toLong
      val index = (indices \ idx \ "total" \ "indexing" \ "index_total").getOrElse(JsNumber(0)).as[JsNumber].value.toLong
      val get = (indices \ idx \ "total" \ "get" \ "total").getOrElse(JsNumber(0)).as[JsNumber].value.toLong
      EsIndexStats(idx, count, deleted, search, index, get)
    }.toSeq

    ret.size
    ret
  }


}
