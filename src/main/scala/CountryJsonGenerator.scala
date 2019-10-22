import java.io.{File, PrintWriter}

import LocalCode.{LocalCode, Localized}
import play.api.libs.json.{Json, OFormat}

import scala.io.Source


object CountryJsonGenerator extends App{
  val gen = new CountryJsonGenerator()
  gen.run()
}

case class CountryCsv( code:String, code3:String, namefr: String, nameen:String, namepl:String)

case class UnifyCountryData(
                             id: String,
                             codeISO2: Option[String],
                             codeISO3: Option[String],
                             localized: Localized[UnifyCountryLocalization]) extends UnifyData

case class UnifyCountryLocalization(name: String)

class CountryJsonGenerator {

  implicit lazy val unifyCountryLocalizationFormat: OFormat[UnifyCountryLocalization] = Json.format[UnifyCountryLocalization]
  implicit lazy val dataFormat: OFormat[UnifyCountryData] = Json.format[UnifyCountryData]


  def run()= {
    println("CSV Edito File : " )
    val editoFile = "/Users/fguay/Downloads/COUNTRY_FR_EN_PL-3.csv" //StdIn.readLine()
    println("Output json Directory : " )
    val output = "/Users/fguay/Downloads/json/country" //StdIn.readLine()

    val countrycsv = Source.fromFile(editoFile, "UTF-8").getLines().toList.map{
      line => {
        line.split(";") match {
          case Array(code, code3, namefr, nameen, namepl) =>
            CountryCsv(code, code3, namefr, nameen, namepl)
        }
      }
    }

    val countries = countrycsv.map{ j=>
      UnifyCountryData(
        j.code.trim,
        Some(j.code.trim),
        Some(j.code3.trim),
        Map[LocalCode, UnifyCountryLocalization](
          LocalCode.pl ->
            UnifyCountryLocalization(
              j.namepl
            ),
          LocalCode.fr ->
            UnifyCountryLocalization(
              j.namefr
            ),
          LocalCode.en ->
            UnifyCountryLocalization(
              j.nameen
            )
        )
      )
    }

    countries.map {
      country => {
        val js = Json.toJson(country)
        val writer = new PrintWriter(new File(s"$output/${country.id}.json" ))
        writer.write(Json.prettyPrint(js))
        writer.close()
      }
    }

  }
}


