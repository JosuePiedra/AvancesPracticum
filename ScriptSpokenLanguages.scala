package Scripts
import com.github.tototoshi.csv.CSVReader

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.io.File
import com.github.tototoshi.csv._
import play.api.libs.json.{JsArray, JsObject, Json}
object ScriptSpokenLanguages extends App{

  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders();
  reader.close()

  case class Spoken_languages(
    iso639_1 : String,
    name : String
  )

  val spokenLanData = data
    .map(row => row("spoken_languages"))
    .map(text => Json.parse(text))
    .flatMap(_.as[JsArray].value)
    .map(_.as[JsObject])
    .map(row => Spoken_languages(
      row("iso_639_1").as[String],
      row("name").as[String]
    ))
    .distinct


  val SQL_INSERT_PATTERN =
    """INSERT INTO spoken_languages(`iso639_1`, `name`
      |VALUES
      |('%s', '%s');
      |""".stripMargin

  val scriptData = spokenLanData
    .map(spokenLanguages => SQL_INSERT_PATTERN.formatLocal(java.util.Locale.US,
      spokenLanguages.iso639_1,
      spokenLanguages.name
    ))

  val scriptFile = new File("/Users/josue/Desktop/Universidad/Programación/ScriptsDB/SpokenLanguages")
  if (scriptFile.exists()) scriptFile.delete()

  scriptData.foreach(insert =>
    Files.write(Paths.get("/Users/josue/Desktop/Universidad/Programación/ScriptsDB/SpokenLanguages"),
      insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )




}
