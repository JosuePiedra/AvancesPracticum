package AnalisisBasicas

import com.github.tototoshi.csv._
import java.io.File

object ACNumericas extends App{
  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()


  //Funciones
  def maximoValor(Lista : List[Double]) : Double = Lista.filter(x => x != 0).max
  def minimoValor(Lista : List[Double]) : Double = Lista.filter(x => x != 0).min
  def promedio(Lista : List[Double]): Double = Lista.filter(x => x != 0).sum / Lista.count(x => x != 0)


  //Estadísticas básicas de la columna "Budget"
  val budget = data
    .flatMap(elem=> elem.get("budget")).map(_.toDouble)

    //La película que gastó el mayor presupuesto

  printf("%20s\n", "BUDGET")
  println("- Presupuesto máximo gastado: " + maximoValor(budget))
    //La película que gastó el mayor presupuesto
  println("- Presupuesto mínimo gastado (sin tomar en cuenta el 0): " + minimoValor(budget))
    //Promedio del presupuesto invertido
  println("- Promedio sin tomar en cuenta el 0: " + promedio(budget))
  println()

  //Estadísticas básicas de la columna "popularity"
  val popularity= data
    .flatMap(elem => elem.get("popularity")).map(_.toDouble)
  printf("%25s\n", "POPULARITY")

    //La película que tiene la mayor popularidad
  println("- Popularidad máxima: " + maximoValor(popularity))
    //La película que tiene la menor popularidad
  println("- Popularidad mas baja (sin tomar en cuenta el 0): " + minimoValor(popularity))
    //Promedio de la popularidad
  println("- Promedio de popularidad sin tomar en cuenta el 0: " + promedio(popularity))
  println("")


  //Estadísticas básicas de la columna "revenue"
  val revenue = data
    .flatMap(elem => elem.get("revenue")).map(_.toDouble)

  printf("%25s\n", "Revenue")
  //La película que tiene la mayor recaudación
  println("- La recaudación mayor: " + maximoValor(revenue))
  //La película que tiene la menor recaudación
  println("- La recaudación menor (sin tomar en cuenta el 0): " + minimoValor(revenue))
  //Promedio de la recaudación
  println("- Promedio de recaudaciones sin tomar en cuenta el 0: " + promedio(revenue))
  println("")


  //Estadísticas básicas de la columna "runtime"
  val runtime = data
    .flatMap(elem => elem.get("revenue")).map(_.toDouble)

  printf("%25s\n", "Revenue")
  //La película que tiene la mayor duración
  println("- La mayor duración:  " + maximoValor(runtime))
  //La película que tiene la menor duración
  println("- La menor duración (sin tomar en cuenta el 0): " + minimoValor(runtime))
  //Promedio de la duración
  println("- Promedio de menor duración: " + promedio(runtime))
  println("")

  //Estadísticas básicas de la columna "runtime"
  val voteAverage = data
    .flatMap(elem => elem.get("vote_average")).map(_.toDouble)

  printf("%25s\n", "Vote Average")
  //La película que tiene la mayor duración
  println("- El puntaje mas alto:  " + maximoValor(voteAverage))
  //La película que tiene la menor duración
  println("- El puntaje mas bajo (sin tomar en cuenta el 0): " + minimoValor(voteAverage))
  //Promedio de la duración
  println("- Promedio de votaciones: " + promedio(voteAverage))
  println("")

  val voteCount = data
    .flatMap(elem => elem.get("vote_count")).map(_.toDouble)

  printf("%25s\n", "Vote Count")
  //La película que tiene la mayor duración
  println("- La cantidad más alta de votaciones: " + maximoValor(voteCount))
  //La película que tiene la menor duración
  println("- La cantidad más baja de votaciones (sin tomar en cuenta el 0): " + minimoValor(voteCount))
  //Promedio de la duración
  println("- Promedio de cantidad de votos: " + promedio(voteCount))
  println("")


}


