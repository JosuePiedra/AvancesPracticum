package AnalisisBasicas

import com.cibo.evilplot.plot.BarChart
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.{DefaultElements, DefaultTheme}
import com.cibo.evilplot.plot.aesthetics.Theme
import com.github.tototoshi.csv.CSVReader
import play.api.libs.json.Json

import java.io.File
import scala.util.Try
import scala.util.matching.Regex

object RepresetancionBarChart extends App{

  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  implicit val theme: Theme = DefaultTheme.copy(
    elements = DefaultElements.copy(categoricalXAxisLabelOrientation = 90)
  )

  //Función que sirve para crear los gráficos
  def barChart(data: List[(String, Double)], Titulo: String, NombreArchivo: String) = {
    BarChart(data.take(10).map(_._2))
      .title(Titulo)
      .xAxis(data.take(10).map(_._1))
      .yAxis()
      .frame()
      .yLabel("Productions")
      .bottomLegend()
      .render()
      .write(new File("/Users/josue/Desktop/Universidad/Programación/IMGScripts/" + NombreArchivo + ".png")
      )
  }

  //Función que sirve para crear tops, siempre y cuando la fila  sea Json
  def topJson(nombreColumna: String, columnaNombreJson: String) = {
    data
      .flatMap(row => row.get(nombreColumna))
      .map(x => Json.parse(x))
      .flatMap(jsonData => jsonData \\ columnaNombreJson)
      .map(jsValue => jsValue.as[String])
      .groupBy(identity)
      .filter(x => !x._1.isBlank)
      .map { case (keyword, lista) => (keyword, lista.length.toDouble) }
      .toList
      .sortBy(_._2)
      .reverse
  }

  //Función que sirve para crear tops, siempre y cuando la fila no sea Json
  def top(valorColumna: String,nombreColumna: String ) = {
    data
      .map(row => (row(nombreColumna), row(valorColumna)))
      .filter(!_._1.isBlank )
      .filter(!_._2.isBlank)
      .map { case (nombre, valor) => (nombre, valor.toDouble) }
      .sortBy(_._2)
      .reverse
  }

  def topUnicaFila(nombreColumna: String)  = {
    data
      .map(row => row.get(nombreColumna))
      .groupBy(identity)
      .filter(_._1.isDefined)
      .map(x => (x._1.get, x._2.length.toDouble))
      .toList
      .sortBy(_._2)
      .reverse
  }

  //Se crea el barchar de budget
  barChart(top("budget", "original_title"), "Top Budget", "budgetBC")
  barChart(top("popularity", "original_title"), "Top Popularity", "popularityBC")
  barChart(top("revenue", "original_title"), "Top Revenue", "revenueBC")
  barChart(top("runtime", "original_title"), "Top Runtime", "runtimerevenueBC")
  barChart(top("vote_average", "original_title"), "Top Votation", "vote_averageBC")
  barChart(topJson("production_companies", "name"),"Top Productoras","productorasBC" )
  barChart(topJson("production_countries", "name"),"Top Paises Productoras","topPaisesProBC" )
  barChart(topJson("spoken_languages", "name"),"Top Lenguajes","lenguajesBC" )

  barChart(topUnicaFila("director"), "Top Directors", "directorsBC")


  // Para la columna de tipo Json crew, se debe realizar un análisis especial
  def replacePatternFull(original: String): String = {
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
    .map(text => Json.parse(text))
    .flatMap(jsonData => jsonData \\ "name")
    .map(jsValue => jsValue.as[String])
    .groupBy(identity)
    .filter(x => !x._1.isBlank)
    .map { case (keyword, lista) => (keyword, lista.length.toDouble) }
    .toList
    .sortBy(_._2)
    .reverse

  barChart(crew, "Top Miembros Participaciones","mienbrosParticipacionBC" )


}
