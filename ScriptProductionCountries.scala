package Scripts

import com.github.tototoshi.csv.CSVReader
import play.api.libs.json.{JsArray, JsObject, Json}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

object ScriptProductionCountries extends App{
  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders();
  reader.close()

  case class ProductionCountries(
                               iso3166_1: String,
                               name: String
                             )

  val productionCounData = data
    .map(row => row("production_countries"))
    .map(text => Json.parse(text))
    .flatMap(_.as[JsArray].value)
    .map(_.as[JsObject])
    .map(row => ProductionCountries(
      row("iso_3166_1").as[String],
      row("name").as[String]
    ))
    .distinct


  val SQL_INSERT_PATTERN =
    """INSERT INTO production_countries (`iso3166_1`, `name`
      |VALUES
      |('%s', '%s');
      |""".stripMargin

  val scriptData = productionCounData
    .map(productionCountries => SQL_INSERT_PATTERN.formatLocal(java.util.Locale.US,
      productionCountries.iso3166_1,
      productionCountries.name
    ))

  val scriptFile = new File("/Users/josue/Desktop/Universidad/Programación/ScriptsDB/ProductionCountries")
  if (scriptFile.exists()) scriptFile.delete()


  scriptData.foreach(insert =>
    Files.write(Paths.get("/Users/josue/Desktop/Universidad/Programación/ScriptsDB/ProductionCountries"),
      insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )


}
