package thinkstats

import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, DoubleType, DataType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import thinkstats.meta.StataData
import org.apache.spark.sql.functions._

import scala.collection
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable
import scala.util.Try

/**
 * Created by m102417 on 12/23/16.
 */
class ThinkStats {


  /**
   * note:
   *    http://stackoverflow.com/questions/35243744/get-specific-row-from-spark-dataframe
   *    http://stackoverflow.com/questions/26828815/how-to-get-element-by-index-in-spark-rdd-java
   *
   * index a dataframe with an increasing numerical key so that we can rapidly get a particular row from the dataframe using lookup
   * @param df
   * @return
   */
  def index(df:DataFrame): RDD[(Long,Row)] = {
    var indexedDF = (df.rdd.zipWithIndex().map( row => (row._2, row._1)))
    indexedDF.cache()
    return indexedDF
  }

  /**
   * reads a fixed with data file (e.g. a Stata file)
   * @param spark
   * @param metadataFile
   * @param dataFile
   * @return DataFrame - spark distributed RDD DataFrame
   */
  def ReadFixedWidth(spark:SparkSession, metadataFile:String, dataFile:String): DataFrame ={
    val lines = spark.sparkContext.textFile(dataFile)
    val meta =  new StataData(metadataFile)
    spark.sparkContext.broadcast(meta)
    val jsonlines = lines.map(s => (meta.dataLine2JSON(s)))
    val rows = spark.read.json(jsonlines)
    //val df = spark.createDataFrame(rows,meta.schema()) //not currently using the schema, spark guesses the schema
    return rows
  }

  /**
   * Works just like pandas 'value_counts()' e.g. a unix sort | uniq on a specific field or column
   * @param df
   * @param field the field we will do the value_counts operation to.
   * @return - a dataframe with the counts for each value in the field.
   */
  def valueCounts(df:DataFrame, field:String): DataFrame ={
    return df.select(field).groupBy(field).count().sort(field)
  }

  /**
   * Converts a datframe to a Map object for rapid access.
   * Note: the assumption is that the resulting map is small and can fit on a single machine
   * http://stackoverflow.com/questions/36239791/convert-dataframe-to-a-mapkey-value-in-spark
   * IMPORTANT, KNOW WHAT YOU ARE DOING!  SOME VALUES SHOULD BE DELETED IN THE CONVERSION TO MAP IF THEY ARE NOT UNIQUE
   * IN THE ORIGNAL DATAFRAME.  Use a lambda to create a unique key on the dataframe if you are not sure!
   * @param df
   * @param fieldKey - the field from the original dataframe to use as a key
   * @param valueKey - the field from the original dataframe to use as the value
   * @return
   */
  def map(df:DataFrame, fieldKey:String, valueKey:String): Map[Any, Any] ={
    val a = df.select(new ColumnName(fieldKey),new ColumnName(valueKey)).rdd
    //val a1 = a.take(1) //Row - [S,57000.0]
    val b = a.map( row => (row(0), row(1) ) )
    //val b1 = b.take(1)
    val c = b.collectAsMap()
    return c.toMap
  }


  /**
   * Do an in-place replace of a value with the result from a computation (specified in function)
   * Given a Dataframe with a well defined schema of datatypes and a field to modify, apply a function to all entries in the dataframe and return the modified dataframe
   * @param spark - the spark session currently being used
   * @param df - the dataframe we want to apply the function to
   * @param field - the field in the dataframe we want to modify
   * @param function - the lambda we want to apply to every field in the dataset
   * @return
   */
  def lambda[T](spark:SparkSession, df:DataFrame, field:String, function:T => T): DataFrame ={
    val idx = df.schema.fieldIndex(field)
    val fieldType = df.schema.fields(idx).dataType.typeName
    val encoder = RowEncoder(df.schema)

    val outrdd = df.as(encoder).rdd.map(row => {
      val input = row.get(idx).asInstanceOf[T]
      val compute = function(input)
      var order = new ArrayBuffer[Any]()
      var i = 0
      while( i < row.size ){
        val item = row.get(i)
        if(i == idx){
          order.+=(compute)
        }else {
          order.+=(item)
        }
        i = i + 1
      }
      Row.fromSeq(order.toSeq)
    })
    val modified = spark.createDataFrame(outrdd,df.schema)
    return modified
  }

  /**
   * Takes a dataframe, adds a column (outField), computes a function on field1 + field2, and puts the result in the new column
   * Great resource on data frame functions!
   * please note that this function should be fast in many cases, but spark can not speed it up using push-down optimization, so
   * it will be a bit slower than if we could more explicity define the function.
   * https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-sql-udfs.html
   * @param spark
   * @param df
   * @param field1
   * @param field2
   * @param outField
   * @param function
   * @return
   */
  def lambda2D[Double](spark:SparkSession, df:DataFrame, field1:String, field2:String, outField:String, function:(Double,Double) => Double): DataFrame ={
    import spark.implicits._
    val f = udf(function, DoubleType)
    val outdf = df.withColumn(outField, f(new ColumnName(field1), new ColumnName(field2)))
    return outdf
  }


  /**
   * casting a column: http://stackoverflow.com/questions/29383107/how-to-change-column-types-in-spark-sqls-dataframe
   * use: val df2 = castColumnTo( df, "year", IntegerType )
   * @param df - dataframe we are modifying
   * @param field - the field we are casting
   * @param tpe - the type we are casting to e.g. IntegerType, DoubleType, ect.
   * @return
   */
  def cast( df: DataFrame, field: String, tpe: DataType ) : DataFrame = {
    df.withColumn( field, df(field).cast(tpe) )
  }

  def count(df:DataFrame, field:String): Int = {
    val mx = map(df.describe(field), "summary", field)
    val db = mx.get("count").getOrElse(null)
    return Integer.parseInt(db.toString)
  }

  /**
   * Calculates the max value for a given column
   * Note: this is the most direct way to calculate max, but its not efficent if we want to
   * calculate max, min ect all at the same time.
   * this does not work:
   *  df.select(field).groupBy(field).max()
   * trying this:
   * http://stackoverflow.com/questions/37016427/get-the-max-value-for-each-key-in-a-spark-rdd
   * @param df
   * @param field
   * @return
   */
  def max(df:DataFrame, field:String) : Option[Double] = {
    val mx = map(df.describe(field), "summary", field)
    val db = mx.get("max").getOrElse(null)
    return parseDouble(db.toString())
  }

  def min(df:DataFrame, field:String) : Option[Double] = {
    val mx = map(df.describe(field), "summary", field)
    val db = mx.get("min").getOrElse(null)
    return parseDouble(db.toString())
  }

  def mean(df:DataFrame, field:String) : Option[Double] = {
    val mx = map(df.describe(field), "summary", field)
    val db = mx.get("mean").getOrElse(null)
    return parseDouble(db.toString())
  }

  def stddev(df:DataFrame, field:String) : Option[Double] = {
    val mx = map(df.describe(field), "summary", field)
    val db = mx.get("stddev").getOrElse(null)
    return parseDouble(db.toString())
  }



  def parseDouble(s: String): Option[Double] = Try { s.toDouble }.toOption

}
