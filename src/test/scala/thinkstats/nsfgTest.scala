package thinkstats;

import _root_.junit.framework.TestCase
import org.apache.spark
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest._



@RunWith(classOf[JUnitRunner])
class nsfgTest extends FlatSpec with Matchers {

  val thinkStats = new ThinkStats()
  //Initialize App
  val spark = SparkSession
    .builder()
    .appName("nsfg")
    .master("local[2]")
    .config("spark.executor.memory","1g")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN") //ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

  //Initialize Class
  val n = new nsfg

  //data
  val dct_file = "src/test/resources/2002FemPreg.dct"
  val dat_file = "src/test/resources/2002FemPreg.dat.gz"

  "A nsfg" should "load the data into a spark dataframe" in {
    import spark.implicits._
    val df = n.ReadFemPreg(spark, dct_file, dat_file)
    df.printSchema()
    //df shape - (13593, 244)
    df.show(1)
    df.count() should be (13593)
    df.columns.length should be (244) //not 243 because we add an additional column
// getting all the data works like this
//    val data = df.collect()
//    for(row <- data){
//      println(row)
//    }

//
//    assert df.caseid[13592] == 12571
    val indexedDF = thinkStats.index(df.select("caseid")) //create an index only on the caseids
    //println(indexedDF.lookup(13592).head.get(0)) //lookup the caseid value for row 13592
    val r = indexedDF.lookup(13592).head.getString(0)  //todo: perhaps we should provide an easier way to use this interface?
    Integer.parseInt(r) should be (12571)

//value_counts() look like this:
//    1     5033
//    2     3766
//    3     2334
//    4     1224
//    5      613
//    6      308
//    7      158
//    8       78
//    9       38
//    10      17
//    11       8
//    12       5
//    13       3
//    14       3
//    18       1
//    19       1
//    17       1
//    16       1
//    15       1
//    Name: pregordr, dtype: int64

//    assert df.pregordr.value_counts()[1] == 5033
    //note the .take(1)(0)(1) is just to unbox the dataframe returned
    //thinkStats.valueCounts(df, "pregordr").show(20)
    thinkStats.valueCounts(df, "pregordr").take(1)(0)(1) should be (5033)
//    assert df.nbrnaliv.value_counts()[1] == 8981
    //thinkStats.valueCounts(df, "nbrnaliv").show(20)
//      +--------+-----+
//      |nbrnaliv|count|
//      +--------+-----+
//      |     1.0| 8981|
//      |     2.0|  138|
//      |     3.0|   14|
//      |     4.0|    5|
//      |     5.0|    6|
//      |     9.0|    4|
//      |     NaN| 4445|
//      +--------+-----+
    thinkStats.valueCounts(df, "nbrnaliv").take(1)(0)(1) should be (8981)
//    assert df.babysex.value_counts()[1] == 4641
    //thinkStats.valueCounts(df, "babysex").show(20)
//      +-------+-----+
//      |babysex|count|
//      +-------+-----+
//      |    1.0| 4641|
//      |    2.0| 4500|
//      |    7.0|    1|
//      |    9.0|    2|
//      |    NaN| 4449|
//      +-------+-----+
    thinkStats.valueCounts(df, "babysex").take(2)(0)(1) should be (4641)
//    assert df.birthwgt_lb.value_counts()[7] == 3049
    //thinkStats.valueCounts(df, "birthwgt_lb").show(20)
//      +-----------+-----+
//      |birthwgt_lb|count|
//      +-----------+-----+
//      |        0.0|    8|
//      |        1.0|   40|
//      |        2.0|   53|
//      |        3.0|   98|
//      |        4.0|  229|
//      |        5.0|  697|
//      |        6.0| 2223|
//      |        7.0| 3049|
//      |        8.0| 1889|
//      |        9.0|  623|
//      |       10.0|  132|
//      |       11.0|   26|
//      |       12.0|   10|
//      |       13.0|    3|
//      |       14.0|    3|
//      |       15.0|    1|
//      |        NaN| 4509|
//      +-----------+-----+
    thinkStats.valueCounts(df, "birthwgt_lb").take(9)(7)(1) should be (3049)
//    assert df.birthwgt_oz.value_counts()[0] == 1037
    //thinkStats.valueCounts(df, "birthwgt_oz").show(20)
//      +-----------+-----+
//      |birthwgt_oz|count|
//      +-----------+-----+
//      |        0.0| 1037|
//      |        1.0|  408|
//      |        2.0|  603|
//      |        3.0|  533|
//      |        4.0|  525|
//      |        5.0|  535|
//      |        6.0|  709|
//      |        7.0|  501|
//      |        8.0|  756|
//      |        9.0|  505|
//      |       10.0|  475|
//      |       11.0|  557|
//      |       12.0|  555|
//      |       13.0|  487|
//      |       14.0|  475|
//      |       15.0|  378|
//      |        NaN| 4554|
//      +-----------+-----+
    thinkStats.valueCounts(df, "birthwgt_oz").take(3)(0)(1) should be (1037)
//    assert df.prglngth.value_counts()[39] == 4744
    //thinkStats.valueCounts(df, "prglngth").show(50)
//      +--------+-----+
//      |prglngth|count|
//      +--------+-----+
//      |       0|   15|
//      |       1|    9|
//      |       2|   78|
//      |       3|  151|
//      |       4|  412|
//      |       5|  181|
//      |       6|  543|
//      |       7|  175|
//      |       8|  409|
//      |       9|  594|
//      |      10|  137|
//      |      11|  202|
//      |      12|  170|
//      |      13|  446|
//      |      14|   29|
//      |      15|   39|
//      |      16|   44|
//      |      17|  253|
//      |      18|   17|
//      |      19|   34|
//      |      20|   18|
//      |      21|   37|
//      |      22|  147|
//      |      23|   12|
//      |      24|   31|
//      |      25|   15|
//      |      26|  117|
//      |      27|    8|
//      |      28|   38|
//      |      29|   23|
//      |      30|  198|
//      |      31|   29|
//      |      32|  122|
//      |      33|   50|
//      |      34|   60|
//      |      35|  357|
//      |      36|  329|
//      |      37|  457|
//      |      38|  609|
//      |      39| 4744|
//      |      40| 1120|
//      |      41|  591|
//      |      42|  328|
//      |      43|  148|
//      |      44|   46|
//      |      45|   10|
//      |      46|    1|
//      |      47|    1|
//      |      48|    7|
//      |      50|    2|
//      +--------+-----+
    thinkStats.valueCounts(df, "prglngth").take(40)(39)(1) should be (4744)
//    assert df.outcome.value_counts()[1] == 9148
//    thinkStats.valueCounts(df, "outcome").show(20)
//      +-------+-----+
//      |outcome|count|
//      +-------+-----+
//      |      1| 9148|
//      |      2| 1862|
//      |      3|  120|
//      |      4| 1921|
//      |      5|  190|
//      |      6|  352|
//      +-------+-----+
    thinkStats.valueCounts(df, "outcome").take(1)(0)(1) should be (9148)
//    assert df.birthord.value_counts()[1] == 4413
    //thinkStats.valueCounts(df, "birthord").show(20)
//      +--------+-----+
//      |birthord|count|
//      +--------+-----+
//      |     1.0| 4413|
//      |     2.0| 2874|
//      |     3.0| 1234|
//      |     4.0|  421|
//      |     5.0|  126|
//      |     6.0|   50|
//      |     7.0|   20|
//      |     8.0|    7|
//      |     9.0|    2|
//      |    10.0|    1|
//      |     NaN| 4445|
//      +--------+-----+
    thinkStats.valueCounts(df, "birthord").take(1)(0)(1) should be (4413)
//    assert df.agepreg.value_counts()[22.75] == 100
    //thinkStats.valueCounts(df, "agepreg").show(200)
//      +-------+-----+
//      |agepreg|count|
//      +-------+-----+
//      |  10.33|    1|
//      |   10.5|    1|
//
//      |  22.75|  100|
//      |  22.83|   70|
//    ...
    thinkStats.valueCounts(df, "agepreg").take(135)(132)(1) should be (100) //

//    assert df.totalwgt_lb.value_counts()[7.5] == 302
    val wtdf = thinkStats.valueCounts(df,"totalwgt_lb")
    val wtmap = thinkStats.map(wtdf,"totalwgt_lb","count")
    wtmap.get(7.5).getOrElse(null) should be (302)      //this is a better way to go look at the summary statistics because we can get it directly out of a map

// Note that this python implementation of max(field) is horribly inefficent... changing the method to be fast on a cluster!
    //idea: postsDf.select(avg('score), max('score), count('score)).show()
    //another idea postsDfNew.groupBy('ownerUserId).agg(max('lastActivityDate), max('score)).show(10)
//    weights = df.finalwgt.value_counts()
//    key = max(weights.keys())
//    assert df.finalwgt.value_counts()[key] == 6
    val weightsdf = thinkStats.valueCounts(df,"finalwgt")
    val maxwt = thinkStats.max(df, "finalwgt")
    println(maxwt)
    val weightsmap = thinkStats.map(weightsdf,"finalwgt", "count")
    println(weightsmap)
    var maxcount = Long.MinValue
    for( next <- weightsmap.values){
      val nexti = next.asInstanceOf[Int]
      if( nexti > maxcount){
        maxcount = nexti
      }
    }
   maxcount should be (6)

    //this is how you can look at all variables
    df.describe().show()

    //this is how you look at only one
    df.describe("finalwgt").show()
    //lots of NaN values in the dataset... need to deal with that!
    //http://stackoverflow.com/questions/33900726/count-number-of-non-nan-entries-in-each-column-of-spark-dataframe-with-pyspark

  }



}