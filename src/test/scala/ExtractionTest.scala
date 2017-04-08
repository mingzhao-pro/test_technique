import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Paths

import org.junit.runner._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

/**
  * Created by saga on 4/8/17.
  */
@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite {

  // IntelliJ use .idea/modules as current working directory
  val filePathPre = "../../src/main/resources/"
  val fileName = "/xag.csv"

  test("max time") {
    val maxTime = Extraction.maxTime(Extraction.readFile(fileName))
    val lines = Extraction.readFile(fileName)
    val calculatedLines = lines.map(x => (x._1, Extraction.finalRating(maxTime, x._3, x._2))).filter(_._2 > 0.01)
    val userIdMap = mutable.Map[String, Integer]()
    val productIdMap = mutable.Map[String, Integer]()
    val userProductRatingMap = mutable.Map[(Integer, Integer), Float]()

    var userIdIndice: Integer = -1
    var productIdIndice: Integer = -1

    calculatedLines.foreach(x => {
      val userIdString = x._1._1
      val userIdInt: Integer = userIdMap.get(userIdString) match {
        case Some(i) => i
        case None => {
          userIdIndice += 1
          userIdMap += (userIdString -> userIdIndice)
          userIdIndice
        }
      }

      val productIdString = x._1._2
      val productIdInt: Integer = productIdMap.get(productIdString) match {
        case Some(i) => i
        case None => {
          productIdIndice += 1
          productIdMap += (productIdString -> productIdIndice)
          productIdIndice
        }
      }

      val key = (userIdInt, productIdInt)
      userProductRatingMap.get(key) match {
        case Some(i) => userProductRatingMap += (key -> (i + x._2))
        case None => userProductRatingMap += (key -> x._2)
      }
    })

//    - agg_ratings.csv : userIdAsInteger,itemIdAsInteger,ratingSum
//    - lookup_user.csv : userId,userIdAsInteger
//    - lookup_product.csv : itemId,itemIdAsInteger
    Extraction.csvPrinter(filePathPre + "lookup_user.csv", userIdMap)
    Extraction.csvPrinter(filePathPre + "lookup_product.csv", productIdMap)
    Extraction.resultPrinter(filePathPre + "agg_ratings.csv", userProductRatingMap)

//    println("userIdMap " + userIdMap.size)
//    println("productIdMap " + productIdMap.size)
//    println("finalMap " + userProductRatingMap.size)
  }
}
