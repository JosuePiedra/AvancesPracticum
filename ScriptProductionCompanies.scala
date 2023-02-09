package Scripts

import com.github.tototoshi.csv.CSVReader
import play.api.libs.json.{JsArray, JsObject, Json}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

object ScriptProductionCompanies extends App{
  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders();
  reader.close()

  case class ProductionCompanies(
                                  id: Int,
                                  name: String
                                )

  val productionCompaniesData = data
    .map(row => row("production_companies"))
    .map(text => Json.parse(text))
    .flatMap(_.as[JsArray].value)
    .map(_.as[JsObject])
    .map(row => ProductionCompanies(
      row("id").as[Int],
      row("name").as[String]
    ))
    .distinct


  val SQL_INSERT_PATTERN =
    """INSERT INTO production_companies (`IDProdC`, `name`
      |VALUES
      |('%d', '%s');
      |""".stripMargin


  val scriptData = productionCompaniesData
    .map(spokenLanguages => SQL_INSERT_PATTERN.formatLocal(java.util.Locale.US,
      spokenLanguages.id,
      spokenLanguages.name
    ))

  val scriptFile = new File("/Users/josue/Desktop/Universidad/Programación/ScriptsDB/ProductionCompanies")
  if (scriptFile.exists()) scriptFile.delete()


  scriptData.foreach(insert =>
    Files.write(Paths.get("/Users/josue/Desktop/Universidad/Programación/ScriptsDB/ProductionCompanies"),
      insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )




}
