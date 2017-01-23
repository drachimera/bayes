package thinkstats.meta

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class StataLineTest extends FlatSpec with Matchers {


  //"We should be able to pares a Stata file with "


  "A StataLine" should "parse values in a metadataline properly" in {
    val meta = new StataLine("    _column(9)      str12                             caseid  %12s  \"RESPONDENT ID NUMBER\"")
    meta.columnStart should be (9)
    meta.datatype should be ("str")
    meta.name should be ("caseid")
    meta.columnEnd should be (21)
    meta.description should be ("RESPONDENT ID NUMBER")

    val meta2 = new StataLine("  _column(4578)       byte                              METRO   %1f  \"PLACE OF RESIDENCE (METROPOLITAN-NON-METROPOLITAN) (RECODE)\"")
    meta2.columnStart should be (4578)
    meta2.datatype should be ("byte")
    meta2.name should be ("METRO")
    meta2.columnEnd should be (4579)
    meta2.description should be ("PLACE OF RESIDENCE (METROPOLITAN-NON-METROPOLITAN) (RECODE)")


  }



}