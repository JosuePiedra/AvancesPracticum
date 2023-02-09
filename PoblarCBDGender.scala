package PoblarBaseDeDatos

import scalikejdbc._
import com.github.tototoshi.csv.CSVReader
import play.api.libs.json.{JsArray, JsObject, Json}
import java.io.File

object PoblarCBDGender extends App{
  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/movie_dataset", "root", "abcd1234")
  implicit val session: DBSession = AutoSession


  val genre = data
    .flatMap(elem => (elem.get("genres")))
    .map(x => x
    .replace("Science Fiction", "Science_Fiction"))
    .flatMap(_.split(" "))
    .filter(_.nonEmpty)
    .distinct

  genre.foreach { name =>
    sql"INSERT INTO genre(name) VALUES ($name)".update.apply()
  }


}
