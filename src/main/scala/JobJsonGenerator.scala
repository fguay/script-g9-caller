import java.io.{File, PrintWriter}

import LocalCode.{LocalCode, Localized}
import play.api.libs.json.{Json, OFormat}

import scala.io.Source


object JobJsonGenerator extends App{
  val gen = new JobJsonGenerator()
  gen.run()
}

case class JobCsv( namefr:String, prefixfr: String, nameen:String, prefixen:String, namepl:String, prefixpl:String)

case class UnifyJobData(
                         id: String, localized: Localized[UnifyJobLocalization]
                         ) extends UnifyData

case class UnifyJobLocalization(name: String, prefix: String)



class JobJsonGenerator {

  implicit lazy val unifyJobLocalizationFormat: OFormat[UnifyJobLocalization] = Json.format[UnifyJobLocalization]
  implicit lazy val dataFormat: OFormat[UnifyJobData] = Json.format[UnifyJobData]


  def run()= {
    println("CSV Edito File : " )
    val editoFile = "/Users/fguay/Downloads/JOB_FR_EN_PL-2.csv" //StdIn.readLine()
    println("Output json Directory : " )
    val output = "/Users/fguay/Downloads/json/job" //StdIn.readLine()

    val jobscsv = Source.fromFile(editoFile, "UTF-8").getLines().toList.map{
      line => {
        line.split(";") match {
          case Array(namefr:String, prefixfr: String, nameen:String, prefixen:String, namepl:String, prefixpl:String) =>
            JobCsv(namefr, prefixfr, nameen, prefixen, namepl, prefixpl)
        }
      }
    }

    val jobs = jobscsv.map{ j=>
      UnifyJobData(
        j.nameen.toLowerCase.trim.replace(" ", "_"),
        Map[LocalCode, UnifyJobLocalization](
          LocalCode.pl ->
            UnifyJobLocalization(
              j.namepl,
              j.prefixpl
            ),
          LocalCode.fr ->
            UnifyJobLocalization(
              j.namefr,
              j.prefixfr
            ),
          LocalCode.en ->
            UnifyJobLocalization(
              j.nameen,
              j.prefixen
            )
        )
        //Some("Job")
      )
    }

    jobs.map {
      job => {
        val js = Json.toJson(job)
        val writer = new PrintWriter(new File(s"$output/${job.id}.json" ))
        writer.write(Json.prettyPrint(js))
        writer.close()
      }
    }

  }
}


