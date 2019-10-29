import java.io.{File, PrintWriter}

import org.slf4j.LoggerFactory
import play.api.libs.json.{JsArray, Json, OFormat}

import scala.io.Source
import scala.util.Try

object HapiQLSchemaGenertor extends App{

  val h = new HapiQLSchemaGenertor()
  h.start()
}

case class SchemaTypeCsv(key:String, dtype:String, autocompleteEnabled:Boolean, autocompleteCache:Option[Boolean], autocompleteValues:Option[Array[String]], autocompleteOperator:Option[Array[String]], autocompleteQueryService:Option[String], autocompleteQueryResource:Option[String], autocompleteQueryParams:Option[String],autocompleteUnits:Option[Array[String]], traductionFr:String, traductionEn:String)

case class SchemaField(key:String, dtype:String, autocomplete:AutoCompleteField, operator: Option[Array[String]])
case class AutoCompleteField(enabled:Boolean, cache:Option[Boolean], units:Option[Array[String]], query:Option[AutoCompleteQuery], values: Option[Array[String]])
case class AutoCompleteQuery(service:String, resource:String, params:String)




class HapiQLSchemaGenertor {


  val logger = LoggerFactory.getLogger(classOf[HapiQLSchemaGenertor])
  import scala.concurrent.ExecutionContext.Implicits._

  implicit lazy val autoCompleteQueryFormat: OFormat[AutoCompleteQuery] = Json.format[AutoCompleteQuery]
  implicit lazy val autoCompleteFieldFormat: OFormat[AutoCompleteField] = Json.format[AutoCompleteField]
  implicit lazy val schemaFieldFormat: OFormat[SchemaField] = Json.format[SchemaField]



  def start() : Unit = {
    println("Grammar CSV File : " )
    val filename = "/Users/fguay/schema-hapiql.csv" //StdIn.readLine()
    val output = "/Users/fguay"
    val fieldsCsv = Source.fromFile(filename).getLines.toList.map {
      line => {
        line.split(";") match {
          case Array(key: String, dtype: String, autocompleteEnabled: String, autocompleteCache: String, autocompleteValues: String, autocompleteOperator: String, autocompleteQueryService: String, autocompleteQueryResource: String, autocompleteQueryParams: String, autocompleteQueryUnits: String, traductionFr: String, traductionEn: String) => {
            SchemaTypeCsv(key,
              dtype,
              Try(autocompleteEnabled.toBoolean).getOrElse(false),
              Try(autocompleteCache.toBoolean).toOption,
              toOptionArray(autocompleteValues),
              toOptionArray(autocompleteOperator),
              toOptionString(autocompleteQueryService),
              toOptionString(autocompleteQueryResource),
              toOptionString(autocompleteQueryParams),
              toOptionArray(autocompleteQueryUnits),
              traductionFr,
              traductionEn)
          }
        }
      }
    }

    val fields = fieldsCsv.map {
      f => {
        SchemaField(
          f.key,
          f.dtype,
          AutoCompleteField(
            f.autocompleteEnabled,
            f.autocompleteCache,
            f.autocompleteUnits,
            (f.autocompleteQueryService, f.autocompleteQueryResource, f.autocompleteQueryParams) match {
              case (Some(service), Some(ressource), Some(params)) => Some(AutoCompleteQuery(
                service, ressource, params
              ))
              case _ => None
            },
            f.autocompleteValues
          ),
          f.autocompleteOperator
        )
      }
    }

    logger.info(fields.toString())

    val f = fields.map {
      field => {
        Json.toJson(field)
      }
    }

    val writer = new PrintWriter(new File(s"$output/schema.json" ))
    writer.write(Json.prettyPrint(JsArray(f)))
    writer.close()

  }

  def toOptionString(s:String) : Option[String] = {
    s match {
      case s if(!s.trim.isEmpty) => Some(s.replace("\"",""))
      case _ => None
    }
  }

  def toOptionArray(s:String) : Option[Array[String]] = {
    toOptionString(s).map{ s => s.split(",")}
  }




}
