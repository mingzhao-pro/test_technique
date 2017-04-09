import org.junit.runner._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by saga on 4/8/17.
  */
@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite {

  test("dispatch file") {
    Extraction.fileDispatcher("/xag.csv")
  }
}
