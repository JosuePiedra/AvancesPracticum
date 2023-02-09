package Scripts

import com.github.tototoshi.csv.CSVReader
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}


object ScriptGenre extends App{

  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders();
  reader.close()
  val genre = data.flatMap(elem => (elem.get("genres"))).flatMap(x => x.split(" "))

  val genre2 = genre.filter(!_.contains("Science"))
    .map(x => x.replace("Fiction", "Science Fiction"))

  case class Genre(
                    IDGenre: String,
                  )


  val genreData = genre2
    .map(row => Genre(
      row
    ))
    .distinct


  val SQL_INSERT_PATTERN =
    """INSERT INTO production_companies (`IDGenre`
      |VALUES
      |('%s');
      |""".stripMargin


  val scriptData = genreData
    .map(genreData => SQL_INSERT_PATTERN.formatLocal(java.util.Locale.US,
      genreData
    ))

  val scriptFile = new File("/Users/josue/Desktop/Universidad/Programación/ScriptsDB/Genre")
  if (scriptFile.exists()) scriptFile.delete()


  scriptData.foreach(insert =>
    Files.write(Paths.get("/Users/josue/Desktop/Universidad/Programación/ScriptsDB/Genre"),
      insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )

}
