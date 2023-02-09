package Scripts

import com.github.tototoshi.csv.CSVReader
import play.api.libs.json.{JsArray, JsObject, JsValue, Json}
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}



object ScriptMovieProducer extends App{

  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders();
  reader.close()

  case class movieProducer(IDProd: String,
                           IDMov: Int)

  val SQL_INSERT_PATTERN =
    """INSERT INTO movie_producer (`IDMov`, `IDProd`
      |VALUES
      |('%d', '%s');
      |""".stripMargin

  val ProductionCom = data
    .map(row => row("production_companies"))
    .map(text => Json.parse(text))
    .map(_.as[List[JsValue]])
    .map(x => (x.flatMap(x => x \\ "name")))


  val movieId = data
    .map(row => row("id"))

  val movieProduction = movieId.zip(ProductionCom)

  val result = movieProduction.flatMap { case (id, companies) => companies.map(company => (id, company.as[String])) }

  val result2 = result.map(x =>
    movieProducer(
      x._2,
      x._1.toInt
  ))

  val scriptData = result2
    .map(movieProducer => SQL_INSERT_PATTERN.formatLocal(java.util.Locale.US,
      movieProducer.IDMov,
      movieProducer.IDProd
    ))


  val scriptFile = new File("/Users/josue/Desktop/Universidad/Programación/ScriptsDB/ProducerMovie")
  if (scriptFile.exists()) scriptFile.delete()


  scriptData.foreach(insert =>
    Files.write(Paths.get("/Users/josue/Desktop/Universidad/Programación/ScriptsDB/ProducerMovie"),
      insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )


}
