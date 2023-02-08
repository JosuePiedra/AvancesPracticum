package AnalisisBasicas
import com.github.tototoshi.csv._

import java.io.File
import com.cibo.evilplot.demo.DemoPlots.theme

import com.cibo.evilplot.plot.{ Histogram}

object RepresentacionHistograma extends App{

  val reader = CSVReader.open(new File("/Users/josue/Desktop/Universidad/Practicum/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  def histograma(Lista: List[Double], Titulo: String, NombreArchivo: String) = {
    Histogram(Lista)
      .title(Titulo)
      .xAxis()
      .yAxis()
      .xbounds(1916.0, 2018.0)
      .render()
      .write(new File("/Users/josue/Desktop/Universidad/Programación/IMGScripts/" + NombreArchivo + ".png"))
  }


 def columnaNumerica (nombreColumna : String) ={
   data.flatMap(elem => elem.get(nombreColumna)).map(_.toDouble)
 }

  histograma(columnaNumerica("budget"), "Top 10 presupuestos mas altos", "budgetsRG")




}
