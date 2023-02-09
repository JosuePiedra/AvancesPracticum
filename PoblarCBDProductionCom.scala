package PoblarBaseDeDatos

import scalikejdbc._
import com.github.tototoshi.csv.CSVReader
import play.api.libs.json.{JsArray, JsObject, Json}
import java.io.File

object PoblarCBDProductionCom extends App {
  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/movie_dataset", "root", "abcd1234")
  implicit val session: DBSession = AutoSession

  val ProductionCom = data
    .map(row => row("production_companies"))
    .map(text => Json.parse(text))
    .flatMap(_.as[JsArray].value)
    .map(_.as[JsObject])
    .map(jsObj => (jsObj("id").as[Int], jsObj("name").as[String]))
    .distinct



  println(ProductionCom)
/*
  ProductionCom.foreach {case (id, name) =>
    sql"INSERT INTO production_companies(id, name) VALUES ($id, $name)".update.apply()
  }




 */
}