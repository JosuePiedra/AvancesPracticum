package PoblarBaseDeDatos
import scalikejdbc._
import com.github.tototoshi.csv.CSVReader
import play.api.libs.json.{JsArray, JsObject, Json}
import java.io.File

object PoblarCBDSpokenLan extends App{
  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/movie_dataset", "root", "abcd1234")
  implicit val session: DBSession = AutoSession

  val SpokenLanguages = data
    .map(row => row("spoken_languages"))
    .map(text => Json.parse(text))
    .flatMap(_.as[JsArray].value)
    .map(_.as[JsObject])
    .map(jsObj => (jsObj("iso_639_1").as[String], jsObj("name").as[String]))
    .distinct


  SpokenLanguages.foreach { case (iso3166, name) =>
    sql"INSERT INTO spoken_languages(iso639, name) VALUES ($iso3166, $name)".update.apply()
  }

}
