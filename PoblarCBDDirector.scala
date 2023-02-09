package PoblarBaseDeDatos
import scalikejdbc._
import com.github.tototoshi.csv.CSVReader
import play.api.libs.json.{JsArray, JsObject, Json}
import java.io.File


object PoblarCBDDirector extends App{
  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/movie_dataset", "root", "abcd1234")
  implicit val session: DBSession = AutoSession

  val director = data
    .map(row => row("director"))
    .map(text => Json.parse(text))
    .flatMap(_.as[JsArray].value)
    .map(_.as[JsObject])
    .map(jsObj => jsObj("name").as[String])
    .distinct


  director.foreach { case id =>
    sql"INSERT INTO director(Director_name) VALUES ($id)".update.apply()
  }

}
