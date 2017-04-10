import java.io.{BufferedWriter, File, FileWriter}
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.temporal.ChronoUnit

import scala.collection.mutable
import scala.io.Source

/**
  * Created by saga on 4/6/17.
  */
object Extraction {

  // IntelliJ use .idea/modules as current working directory
  val FilePathPre = "../../src/main/resources/"
  val UserIdFile = "lookup_user.csv"
  val ProductIdFile = "lookup_product.csv"
  val RatingFile = "agg_ratings.csv"

  def readFile(file: String): Iterator[((String, String), String, String)] = {
    val Splitter = ","
    Source.fromInputStream(this.getClass.getResourceAsStream(file)).getLines()
          .map(_.split(Splitter)).map(x => ((x(0), x(1)), x(2), x(3))) // (userId, ItemId), rating, time
  }

  def filePrinter(fileName: String, lines: mutable.Map[String, Int]) = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    lines.toArray.sortWith((a, b) => a._2 < b._2)
         .map(x => x._1 + "," + x._2.toString + "\n")
         .foreach(bw.write)
    bw.close()
  }

  def aggFilePrinter(fileName: String, lines: mutable.Map[(Int, Int), Float]) = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    lines.foreach(x => {
         val line = x._1._1.toString + "," + x._1._2.toString + "," + (math.round(x._2 * 100.0) / 100.0).toFloat + "\n"
         bw.write(line)
       })
    bw.close()
  }

  /**
    * * une pénalité multiplicative de 0.95 est appliquée au rating
    *   pour chaque jour d'écart avec le timestamp maximal de input.csv
    *
    * @param nowTime maximal timestamp at input.csv
    * @param pastTime    current rating time
    * @param rating  original rating
    * @return final rating multiplied by 0.95 for every day interval from the maximal timestamp
    */
  def finalRating(nowTime: String, pastTime: String, rating: String): Float = {
    val now =
      LocalDateTime.ofInstant(Instant.ofEpochMilli(nowTime.toLong), ZoneId.systemDefault())
    val past =
      LocalDateTime.ofInstant(Instant.ofEpochMilli(pastTime.toLong), ZoneId.systemDefault())
    val diff = ChronoUnit.DAYS.between(past, now)
    (math.pow(0.95, diff) * rating.toFloat).toFloat
  }

  /**
    *
    * @param file file to extract
    */
  def fileDispatcher(file: String) = {

    /**
      * get idIndice or increment to idIndice and put it to id map
      * @param id  id in String
      * @param idIndice id in Int
      * @param idMap userIdMap or productIdMap
      * @return (indice for id, max idIndice)
      */
    def getIndice(id: String, idIndice: Int, idMap: mutable.Map[String, Int]): (Int, Int) = {
      idMap.get(id) match {
        case Some(i) => (i, idIndice)
        case None => {
          val indice = idIndice + 1
          idMap += (id -> indice)
          (indice, indice)
        }
      }
    }

    // 1. scan the file the find the max time
    val maxTime = readFile(file).reduce((a, b) => if(a._3 > b._3) a else b)._3

    // 2. apply rating condition, calculate rating and return only valid rating lines
    val validLines = readFile(file).map(x => (x._1, finalRating(maxTime.toString, x._3, x._2))).filter(_._2 > 0.01)

    // 3. loop file lines, sum ratings by (userId, productId), and combine id_String and id_Int
    val userIdMap = mutable.Map[String, Int]() // (userId, userIdAsInt)
    val productIdMap = mutable.Map[String, Int]() // (productId, productIdAsInt)
    val userProductRatingMap = mutable.Map[(Int, Int), Float]() // (userIdAsInt, productIdAsInt, ratingSum)

    var userIdIndice = -1
    var productIdIndice = -1

    for (x <- validLines) {
      val userIdString = x._1._1
      val userId = getIndice(userIdString, userIdIndice, userIdMap)
      userIdIndice = userId._2

      val productIdString = x._1._2
      val productId = getIndice(productIdString, productIdIndice, productIdMap)
      productIdIndice = productId._2

      val key = (userId._1, productId._1)
      userProductRatingMap.get(key) match {
        case Some(i) => userProductRatingMap += (key -> (i + x._2))
        case None => userProductRatingMap += (key -> x._2)
      }
    }

    filePrinter(FilePathPre + UserIdFile, userIdMap)
    filePrinter(FilePathPre + ProductIdFile, productIdMap)
    aggFilePrinter(FilePathPre + RatingFile, userProductRatingMap)
  }
}
