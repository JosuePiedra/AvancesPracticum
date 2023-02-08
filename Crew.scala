package AnalisisBasicas
import com.github.tototoshi.csv.CSVReader
import play.api.libs.json._

import com.github.tototoshi.csv._


import java.io.File
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex


object Crew extends App{
  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

 def replacePatternFull(original: String) : String = {
   var txtOr = original
   val pattern1: Regex = "(\\s\"(.*?)\",)".r
   val pattern2: Regex = "([a-z]\\s\"(.*?)\"\\s*[A-Z])".r
   val pattern3: Regex = "(:\\s'\"(.*?)',)".r


   txtOr = pattern2.replaceAllIn(txtOr, m => m.toString().replace("\"", "-u0022"))
   txtOr = pattern1.replaceAllIn(txtOr, m => m.toString().replace("'", "-u0027"))
   txtOr = pattern3.replaceAllIn(txtOr, m => m.toString().replace("\"", "-u0022"))
   txtOr

 }

  val crew = data
    .map(row => row("crew"))
    .map(replacePatternFull)
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(text => Try(Json.parse(text))).count(_.isSuccess)

  println(crew)

  val crewJS = data
    .map(row => row("crew"))
    .map(replacePatternFull)
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(text => Try(Json.parse(text)))

  val departments = crewJS.map(_.get).flatMap(_ \\ "department").map(_.as[String])
  val trabajos = crewJS.map(_.get).flatMap(_ \\ "job").map(_.as[String])



  val idNameList = crewJS
    .filter(_.isSuccess)
    .map(_.get)
    .flatMap(_.as[JsArray].value)
    .map(_.as[JsObject])
    .map(jsObj => (jsObj("id").as[Int],jsObj("name").as[String]))
    .groupBy(identity)
    .map{
      case (tuplas, list) => (tuplas, list.size)
    }
    .filter(_._2 > 1)
    .toList
    .sortBy(_._2)
    .reverse

  println(idNameList)


  val AvatarDepartment = crewJS.map(_.get).head.as[JsValue] \\ "department"
  val AvatarJob = crewJS.map(_.get).head.as[JsValue] \\ "job"



  println("Departamentos de la película Avatar: ")
  println("-----------------------------------------")
  println(AvatarDepartment.map(_.as[String]).distinct.toList + "\n")

  println("Trabajos de la película Avatar: ")
  println("-----------------------------------------")
  println(AvatarJob.map(_.as[String]).distinct.toList + "\n")

  println("Departamentos de todas las películas")
  println("-----------------------------------------")
  println(departments.groupBy(identity).map( x => (x. _1, x._2.length)) + "\n")

  println("Trabajos de todas las películas")
  println("-----------------------------------------")
  println(trabajos.groupBy(identity).map(x => (x._1, x._2.length)) + "\n")

  println(trabajos.groupBy(identity).map(x => (x._1, x._2.length)) + "\n")


  import scala.util.matching.Regex

  /* val names = List(" .replace(\"\\\\u00e1\", \"á\")", ."replace("\\u00c1", "Á")")
  val pattern1: Regex = "\"(.*?)\"".r
  val pattern2: Regex = "\"(.*?)\"".r

  val extractedNames1 = names.flatMap(name => pattern1.findAllIn(name).toList)
  val extractedNames2 = names.flatMap(name => pattern2.findAllIn(name).toList)

  println(extractedNames1)
  println(extractedNames2)

   */



}
