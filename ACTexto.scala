package AnalisisBasicas

import com.github.tototoshi.csv._
import java.io.File

object ACTexto extends App{
  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  //Análisis columna género de pelicula

  val genre = data.flatMap(elem => (elem.get("genres"))).flatMap(x => x.split(" "))

  val genre2= genre.filter(!_.contains("Science")).
    groupBy(x => x.replace("   ", "Science Fiction")).map(x => (x._1, x._2.length))

  printf("%40s\n", "Género películas")
  genre2.foreach(x => println("El género "+ x._1 + " se repite en " + x._2 + " peliculas"))
  println("")
  println("El género " + genre2.minBy(_._2)._1 + " es el menos frecuente")
  println("El género " + genre2.maxBy(_._2)._1 + " es el mas frecuente\n")

  // Análisis columna género de homepage
  val homepage = data.flatMap(elem => (elem.get("homepage")))
  homepage.groupBy(x => x).map(x => (x._1, x._2.length))

  //Análisis de la columna original lenguage
  val originalLenguage = data.flatMap(elem => (elem.get("original_language")))
  val originalLenguage2 = originalLenguage.groupBy(x => x).map(x => (x._1, x._2.length))

  printf("%40s\n", "Idiomas Películas")
  originalLenguage2.foreach(x => println("El idioma " + x._1 + " se repite en " + x._2 + " peliculas"))
  println("")
  println("El idioma " + originalLenguage2.minBy(_._2)._1 + " es el menos frecuente")
  println("El idioma " + originalLenguage2.maxBy(_._2)._1 + " es el mas frecuente\n")


  //Análisis de la columna original_tittle
  val orginalTittle = data.flatMap(elem => (elem.get("original_title")))
  val orginalTittle2 = orginalTittle.groupBy(x => x).map(x => (x._1, x._2.length))

  printf("%40s\n", "Títulos originales")
  orginalTittle2.filter(_._2 > 1).foreach(x => println("El título " + x._1 + " se repite en " + x._2 + " peliculas"))
  println("")

  //Análisis de la columna release_date
  val realeasedDate = data.flatMap(elem => (elem.get("release_date")))
  val realeasedDate2 = realeasedDate.groupBy(x => x).map(x => (x._1, x._2.length))

  // println("Películas estrenada el mismo día: ")
  // realeasedDate2.filter(_._2 > 1).foreach(x => println("El día " + x._1 + " se han estrenado " + x._2 + " peliculas"))
  println("El día que mas estrenos han existido fue: " + realeasedDate2.maxBy(_._2)._1 + " con un total de: " + realeasedDate2.maxBy(_._2)._2)
  println("")


  //Análisis de la columna status
  val status = data.flatMap(elem => (elem.get("status")))
  val status2 = status.groupBy(x => x).map(x => (x._1, x._2.length))

  printf("%40s\n", "Estado de publicación")
  status2.foreach(x => println("Existen " + x._2 + " películas en el estado: " + x._1))

  //Análisis de la columna title
  val title = data.flatMap(elem => (elem.get("title")))
  val title2 = title.groupBy(x => x).map(x => (x._1, x._2.length))

  println("")
  printf("%40s\n", "Títulos")
  title2.filter(_._2 > 1).foreach(x => println("El título " + x._1 + " se repite en " + x._2 + " peliculas"))
  println("\n")


}
