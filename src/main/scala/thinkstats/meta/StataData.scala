package thinkstats.meta

import org.apache.spark.sql.types._
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.mutable
import scala.io.Source
import scala.collection.mutable.{Map => MMap, ArrayBuffer}

/**
 * Created by m102417 on 12/28/16.
 * Basic utilities for reading the data out of a Stata file
 */
class StataData (metaDataFile:String)  extends Serializable {
  var metaFile:String = metaDataFile
  var metadata:mutable.LinkedHashMap[String,StataLine] = mutable.LinkedHashMap[String,StataLine]()

  parseMetadata(metaDataFile)

  /**
   * parses the entire metadata file, putting the results into memory
   * @param filename
   * @return
   */
  def parseMetadata(filename:String): MMap[String,StataLine] ={
    metadata = mutable.LinkedHashMap[String,StataLine]()
    for (line <- Source.fromFile(filename).getLines) {
      if(line.contains("_column")){
        var meta = new StataLine(line)
        metadata.put(meta.name,meta)
      }
    }
    return metadata
  }

  /**
   * reads the archaic format and converts it to json so we can then make a spark dataframe with it or whatever is needed.
   * @param line
   * @return
   */
  def dataLine2JSON(line:String): String ={
    //setup the json object to hold the results from the parse
    val mapper = new ObjectMapper();
    val rootNode = mapper.createObjectNode();
    //parse the data line based on the metadata specification
    for(field <- metadata.values){
      val value = getField(line, field)
      //println(field.name + ":" + value)
      //if the field is empty or null, then don't put it in the resulting json
      //variables can be = "byte","str", "double", "float", "int", "long"
      if(value.length() > 0){
        //based on the datatype, add the field data to the json
        if(field.datatype.equals("byte") || field.datatype.equals("int") || field.datatype.equals("long")){
          rootNode.put(field.name,value.toLong)
        }else if(field.datatype.equals("double") || field.datatype.equals("float")) {
          rootNode.put(field.name, value.toDouble)
        } else {//treat as a string
          rootNode.put(field.name,value)
        }
      }else {
        rootNode.put(field.name, Double.NaN)
      }
    }

    return rootNode.toString()
  }

  /**
   * raw method to get the substring at the metadata location
   * @param line
   * @return
   */
  def getField(line:String, field:StataLine): String = {
    return line.substring(field.columnStart-1,field.columnEnd-1).trim()
  }

  /**
   * returns
   * @return schema object for use in spark encoders
   */
  def schema(): StructType ={
    var buffer = ArrayBuffer[StructField]()
    for (item <- metadata.keys){
      val imeta = metadata.get(item).get //why the heck does scala wrap up the values in Some() objects?
      //"byte","str", "double", "float", "int", "long"
      if(imeta.datatype.toLowerCase().startsWith("byte")) {
        val struct = StructField(item, ByteType)
        buffer.insert(buffer.length, struct)
      } else if (imeta.datatype.toLowerCase().startsWith("double")) {
        val struct = StructField(item, DoubleType)
        buffer.insert(buffer.length, struct)
      } else if (imeta.datatype.toLowerCase().startsWith("float")) {
        val struct = StructField(item, FloatType)
        buffer.insert(buffer.length, struct)
      } else if (imeta.datatype.toLowerCase().startsWith("int")) {
        val struct = StructField(item, IntegerType)
        buffer.insert(buffer.length, struct)
      } else if (imeta.datatype.toLowerCase().startsWith("long")) {
        val struct = StructField(item, LongType)
        buffer.insert(buffer.length, struct)
      } else { //all other cases, we have no option but to do string!
        val struct = StructField(item, StringType)
        buffer.insert(buffer.length, struct)
      }

    }
    //var s = new Vector()
    //return StructType(s.toSeq[StructField])
    val schema = StructType(buffer.toSeq)
    return schema
  }


}
