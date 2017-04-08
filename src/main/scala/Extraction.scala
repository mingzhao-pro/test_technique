import java.io.{BufferedWriter, File, FileWriter}

import org.scalameter._

import scala.collection.mutable
import scala.io.Source

/**
  * Created by saga on 4/6/17.
  */
object Extraction {

  def readFile(file: String) = {
    val lines = Source.fromInputStream(this.getClass.getResourceAsStream(file)).getLines()
    lines.map(_.split(",")).map(x => ((x(0), x(1)), x(2).toFloat, x(3).toLong)) // (userId, ItemId), rating, time
  }

  def maxTime(lines : Iterator[((String, String), Float, Long)]): Long = {
    val lines = readFile("/xag.csv")
    var maxTime = 0L
    for(l <- lines) {
      maxTime = math.max(maxTime, l._3)
    }
    maxTime
  }

  def csvPrinter(fileName: String, lines: mutable.Map[String, Integer]) = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    lines.toArray.sortWith((a, b) => a._2 < b._2)foreach(x => {
      val s = x._1 + "," + x._2.toString
      bw.write(s)
      bw.write("\n")
    })
    bw.close()
  }

  def resultPrinter(fileName: String, lines: mutable.Map[(Integer, Integer), Float]) = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    lines.foreach({x =>
      val s = x._1._1.toString + "," + x._1._2.toString + "," + (math.round(x._2 * 100.0) / 100.0).toString
      bw.write(s)
      bw.write("\n")
    })
    bw.close()
  }

  /**
    * * une pénalité multiplicative de 0.95 est appliquée au rating
    *   pour chaque jour d'écart avec le timestamp maximal de input.csv
    *
    * @param maxTime maximal timestamp at input.csv
    * @param time    current rating time
    * @param rating  original rating
    * @return final rating multiplied by 0.95 for every day interval from the maximal timestamp
    */
  def finalRating(maxTime: Long, time: Long, rating: Float): Float = {
    val diffByDay = math.floor((maxTime - time) / 1000 / 60 / 60 / 24) // int <= double
    (math.pow(0.95, diffByDay) * rating).toFloat
  }

  def main(args: Array[String]): Unit = {
    val lines = Source.fromInputStream(this.getClass.getResourceAsStream("/xag.csv")).getLines()
    val splited = lines.map(_.split(",")).map(x => (x(0), x(1), x(2).toFloat, x(3).toLong)) // (userId, ItemId), rating, time
    val sortedLines = splited.toSeq.sortWith((a, b) => a._4 > b._4)

    //agg_ratings.csv : userIdAsInteger,itemIdAsInteger,ratingSum
    //utilisateur/produit sont uniques
    //- userIdAsInteger : identifiant unique d'un utilisateur (Int)
    //- itemIdAsInteger : identifiant unique d'un produit (Int)
    //- ratingSum : Somme des ratings pour le couple utilisateur/produit (Float)





//    // for temp max time, if rating < 0.01, change to 0
//    val lines = Source.fromInputStream(this.getClass.getResourceAsStream("test.csv")).getLines()
//    // (timeMax, (userId, itemId, rating, time)
////    val maxTime = splited.maxBy(_._4)
//    var maxTime = 0L
//    val splited = lines.map(_.split(",")).map(x => (x(0), x(1), x(2).toFloat, x(3).toLong)) // (userId, ItemId), rating, time
//    val maxs = splited.maxBy(_._4)
//    println("maxTime " + maxTime + " size " + splited.size)

  }
}
