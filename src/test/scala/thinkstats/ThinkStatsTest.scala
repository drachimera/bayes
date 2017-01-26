package thinkstats

import org.apache.spark
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SparkSession, SQLContext, Row}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._

import scala.collection.parallel.mutable

@RunWith(classOf[JUnitRunner])
class ThinkStatsTest extends FlatSpec with Matchers {
  val thinkstats = new ThinkStats()
  val spark = SparkSession
    .builder()
    .appName("ThinkStatsTest")
    .master("local[2]")
    .config("spark.executor.memory","1g")
    .getOrCreate()

  "The lamda function" should "apply a specified function to all elements in a data frame" in {
    //init schema
    val schema = StructType(Seq(
      StructField("year", IntegerType),
      StructField("make", StringType),
      StructField("model", StringType),
      StructField("cost", DoubleType),
      StructField("rebate", DoubleType)
    ))
    //init data
    val rdd = spark.sparkContext.parallelize(
      List( (2012,"Tesla","S",60000.00, 0.0), (1997,"Ford","E350",33705.95, 1000.0), (2015,"Chevy","Volt",33220.18, 500.0))
    )

    // this is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._
    val df = spark.createDataFrame(rdd.toDF().rdd,schema) //note rdd.toDF().rdd looks dumb, but it is actually converting the json to a RDD[Row]

    df.show(3)

    //lambda
    val add1 = (x: Int) => { x+1 }
    val outdf = thinkstats.lambda(spark,df,"year", add1)

    //lambda #2
    val discount = (x:Double) => { x - (x*0.05) } //a 5% discount
    val outdf2 = thinkstats.lambda(spark,outdf,"cost", discount)

    //lambda #3
    val fixChevy = (x:String) => { if (x.equalsIgnoreCase("chevy")) "Chevrolet" else x }
    val outdf3 = thinkstats.lambda(spark,outdf2,"make", fixChevy)

    //lambda #4 - combine two numbers in a calculation
    val realCost = (cost:Double,rebate:Double) => { cost - rebate }
    val outdf4 = thinkstats.lambda2D(spark,outdf3,"cost", "rebate", "finalcost", realCost)

    outdf4.show(3)

    //row0
    outdf4.take(1)(0)(0) should be (2013) //next model year
    outdf4.take(1)(0)(1) should be ("Tesla")
    outdf4.take(1)(0)(2) should be ("S")
    outdf4.take(1)(0)(3) should be (57000.0) //discounted
    outdf4.take(1)(0)(4) should be (0.0)
    outdf4.take(1)(0)(5) should be (57000.0)

    //row1
    outdf4.take(2)(1)(0) should be (1998) //next model year
    outdf4.take(2)(1)(1) should be ("Ford")
    outdf4.take(2)(1)(2) should be ("E350")
    outdf4.take(2)(1)(3) should be (32020.652499999997) //discounted
    outdf4.take(2)(1)(4) should be (1000.0)
    outdf4.take(2)(1)(5) should be (31020.652499999997)

    //row2
    outdf4.take(3)(2)(0) should be (2016) //next model year
    outdf4.take(3)(2)(1) should be ("Chevrolet")
    outdf4.take(3)(2)(2) should be ("Volt")
    outdf4.take(3)(2)(3) should be (31559.171000000002) //discounted
    outdf4.take(3)(2)(4) should be (500.0)
    outdf4.take(3)(2)(5) should be (31059.171000000002)


    val map = thinkstats.map(outdf4, "model", "finalcost")   //note cast, only at this time do we know!
    map.get("S").getOrElse(null) should be (57000.0)    //the object we get back is Scala Some!
    map.get("E350").getOrElse(null) should be (31020.652499999997)
    map.get("Volt").getOrElse(null) should be (31059.171000000002)

    thinkstats.count(outdf4, "finalcost") should be (3)
    thinkstats.max(outdf4, "finalcost").getOrElse(null) should be (57000.0)
    thinkstats.min(outdf4, "finalcost").getOrElse(null) should be (31020.652499999997)
    thinkstats.mean(outdf4, "finalcost").getOrElse(null) should be (39693.2745)
    thinkstats.stddev(outdf4, "finalcost").getOrElse(null) should be (14988.07631312215)

  }

}


