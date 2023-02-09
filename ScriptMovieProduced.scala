package Scripts

import com.github.tototoshi.csv.CSVReader
import play.api.libs.json.{JsValue, Json}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

object ScriptMovieProduced extends App{

  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders();
  reader.close()

  case class MovieProduced(iso3166_1: String,
                           IDMov: Int)

  val SQL_INSERT_PATTERN =
    """INSERT INTO produced_movie (`iso3166_1`, `IDMov`
      |VALUES
      |('%s', '%d');
      |""".stripMargin

  val ProductionCountry = data
    .map(row => row("production_countries"))
    .map(text => Json.parse(text))
    .map(_.as[List[JsValue]])
    .map(x => (x.flatMap(x => x \\ "iso_3166_1")))


  val movieId = data
    .map(row => row("id"))

  val movieProduced = movieId.zip(ProductionCountry)

  val result = movieProduced.flatMap { case (id, companies) => companies.map(company => (id, company.as[String])) }

  val result2 = result.map(x =>
    MovieProduced(
      x._2,
      x._1.toInt
    ))

  val scriptData = result2
    .map(movieProducer => SQL_INSERT_PATTERN.formatLocal(java.util.Locale.US,
      movieProducer.iso3166_1,
      movieProducer.IDMov
    ))


  val scriptFile = new File("/Users/josue/Desktop/Universidad/Programación/ScriptsDB/ProducedMovie")
  if (scriptFile.exists()) scriptFile.delete()


  scriptData.foreach(insert =>
    Files.write(Paths.get("/Users/josue/Desktop/Universidad/Programación/ScriptsDB/ProducedMovie"),
      insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )


}
