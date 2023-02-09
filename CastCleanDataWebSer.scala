package CastWebServices


import com.github.tototoshi.csv.CSVReader
import requests.Response

import java.io.File
import play.api.libs.json._
import com.github.tototoshi.csv._

import scala.util.{Failure, Success, Try}
import scalikejdbc._


object CastCleanDataWebSer extends App{

  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/movie_dataset", "root", "abcd1234")
  implicit val session: DBSession = AutoSession


  def actorsNames(dataRaw: String): Option[String] = {
    val response: Response = requests
      .post("http://api.meaningcloud.com/topics-2.0",
        data = Map("key" -> "425e5917dbfea04f82268d4cd89e111c",
          "lang" -> "en",
          "txt" -> dataRaw,
          "tt" -> "e"),
        headers = Map("content-type" -> "application/x-www-form-urlencoded"))
    Thread.sleep(1000)
    if (response.statusCode == 200) {
      Option(response.text)
    } else
      Option.empty
  }

  val cast = data
    .map(row => row("cast"))
    .filter(_.isBlank)
    .map(StringContext.processEscapes)
    .take(30) //Use un número limitado para hacer sus pruebas, pero, al final debe analizar todos los datos.
    .map(actorsNames)
    .map(json => Try(Json.parse(json.get)))
    .filter(_.isSuccess)
    .map(_.get)
    .flatMap(json => json("entity_list").as[JsArray].value)
    .map(_("form"))
    .map(data => data.as[String])




}
