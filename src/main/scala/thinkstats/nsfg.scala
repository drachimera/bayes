package thinkstats

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types.DoubleType
import thinkstats.ThinkStats


/**
 * Created by m102417 on 12/23/16.
 */
class nsfg {

  var thinkStats = new ThinkStats()

  /**
   * Reads the NSFG pregnancy data.

    sqlContext: a spark sql_context
    dct_file: string file name
    dat_file: string file name

    returns: Spark Data Frame
   */
  def ReadFemPreg(spark:SparkSession, dct_file:String, dat_file:String): DataFrame ={
    val df = thinkStats.ReadFixedWidth(spark,dct_file,dat_file)
    //print(df.first())
    //df.printSchema()
    val df2 = CleanFemPreg(spark, df)
    return df2
  }

  /**
   * cleans up the dataset
   * @param spark
   * @param df
   * @return
   */
  def CleanFemPreg(spark:SparkSession, df:DataFrame): DataFrame ={

    //val idx = df.schema.fieldIndex("agepreg")
    //mother's age is encoded in centiyears; convert to years
    // df.agepreg /= 100.0

    val casted = thinkStats.cast(df, "agepreg", DoubleType)
    val toYears = (age:Double) => { age / 100.00 }
    val yearsFixed = thinkStats.lambda(spark, casted, "agepreg", toYears)

    //# birthwgt_lb contains at least one bogus value (51 lbs)
    //# replace with NaN
    //df.loc[df.birthwgt_lb > 20, 'birthwgt_lb'] = np.nan
    val castwt = thinkStats.cast(yearsFixed, "birthwgt_lb", DoubleType)
    val fixwt = (wt:Double) => { if(wt > 20) Double.NaN else wt }
    val wtFixed = thinkStats.lambda(spark, castwt, "birthwgt_lb", fixwt)

    //# replace 'not ascertained', 'refused', 'don't know' with NaN
    //na_vals = [97, 98, 99]
    val replaceNA = (x:Double) => (if( x < 20 && x >= 0) x else Double.NaN)  //note there is a strange effect on the boundaries if a value is null, lambda takes it as zero
    //df.birthwgt_lb.replace(na_vals, np.nan, inplace=True)
    val fix1 = thinkStats.lambda(spark,
                               thinkStats.cast(wtFixed,"birthwgt_lb", DoubleType),
                               "birthwgt_lb",
                               replaceNA)
    //df.birthwgt_oz.replace(na_vals, np.nan, inplace=True)
    val fix2 = thinkStats.lambda(spark,
                              thinkStats.cast(fix1,"birthwgt_oz", DoubleType),
                              "birthwgt_oz",
                              replaceNA)
    //df.hpagelb.replace(na_vals, np.nan, inplace=True)
    val fix3 = thinkStats.lambda(spark,
                              thinkStats.cast(fix2,"hpagelb", DoubleType),
                              "hpagelb",
                              replaceNA)

    //df.babysex.replace([7, 9], np.nan, inplace=True)
    val fix4 = thinkStats.lambda(spark,
               thinkStats.cast(fix3,"babysex", DoubleType),
               "babysex",
               (x:Double) => (if( x == 7 || x == 9) Double.NaN else x))
    //df.nbrnaliv.replace([9], np.nan, inplace=True)
    val fix5 = thinkStats.lambda(spark,
              thinkStats.cast(fix4,"nbrnaliv", DoubleType),
              "nbrnaliv",
              (x:Double) => (if(x == 9) Double.NaN else x))

    //# birthweight is stored in two columns, lbs and oz.
    //# convert to a single column in lb
    //# NOTE: creating a new column requires dictionary syntax,
    //# not attribute assignment (like df.totalwgt_lb)
    //df['totalwgt_lb'] = df.birthwgt_lb + df.birthwgt_oz / 16.0
    val compute = (lb:Double, oz:Double) => (lb + oz/16.0)
    val fix6 = thinkStats.lambda2D(spark,fix5,"birthwgt_lb", "birthwgt_oz", "totalwgt_lb", compute)


    val modified = fix6

    return modified
  }

  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

}
