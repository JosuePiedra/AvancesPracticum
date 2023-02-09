package PoblarBaseDeDatos

import scalikejdbc._
import com.github.tototoshi.csv.CSVReader
import play.api.libs.json.{JsArray, JsObject, Json}
import java.io.File

object PoblarCBDProductionCountry extends App{
  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/movie_dataset", "root", "abcd1234")
  implicit val session: DBSession = AutoSession

  val ProductionCountry = data
    .map(row => row("production_countries"))
    .map(text => Json.parse(text))
    .flatMap(_.as[JsArray].value)
    .map(_.as[JsObject])
    .map(jsObj => (jsObj("iso_3166_1").as[String], jsObj("name").as[String]))
    .distinct


  ProductionCountry.foreach { case(id, name) =>
    sql"INSERT INTO production_countries(iso3166, name) VALUES ($id, $name)".update.apply()
  }

}
