package AnalisisJson
import com.github.tototoshi.csv.CSVReader
import play.api.libs.json._

import java.io.File


object JSON extends App {
  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  //análisis columna productión_companies
  val json = data.flatMap(elem => (elem.get("production_companies"))).map(Json.parse)
  val names = json.flatMap(_ \\ "name" )
  val mostProduction = names.groupBy(x => x).map(x => (x._1, x._2.length)).toList

  println("   Análisis compañias productoras")
  println(" ===================================")
  println(" La película con más producciones es: ")
  println(mostProduction.maxBy(_._2)._1 + "\n")

  println( "El top 5 de las compañias con más producción son: ")
  println(" - - - - -- - - - - - - - -- ")

  mostProduction.sortWith((x, y) => x._2 > y._2).take(5).foreach( x => println(x._1 + " con "+ x._2) + " producciones")
  //println(mostProduction.sortBy(x => x._2))


}
