package thinkstats.meta

import scala.collection.mutable.Stack

/**
 * Created by m102417 on 12/23/16.
 * Stata dictionary files need to be parsed, the format of a file is as follows:
 * infile dictionary {
_column(1)  str12  caseid    %12s  "RESPONDENT ID NUMBER"
_column(13) byte   pregordr   %2f  "PREGNANCY ORDER (NUMBER)"
}
 *
 */
class StataLine(sline:String) extends Serializable {

  var line:String = sline

  //member variables
  var columnStart:Int = _
  var columnEnd:Int = _
  var datatype:String = _
  //e.g. str12, int, byte
  var name: String = _
  //the short name of the variable
  var description:String = _ //description of what the variable means

  //global variables
  val VAR_TYPES = List("byte","str", "double", "float", "int", "long")

  //initialize the member variables...
  parse(sline)

  /**
   * parses the start position, where a data field begins
   * @param colToken a string like _column(13)
   * @return an integer of the start position e.g. 13 for the example above
   */
  def parseStart(colToken:String): Int ={
    val strippedToken = colToken.replace("_column(","").replace(")","")
    return new Integer(strippedToken)
  }

  /**
   * parses the datatype of the variable
   * @param colToken
   * @return
   */
  def parseDatatype(colToken:String): String ={
    //VAR_TYPES = Array("Hello","World")
    for(vtype <- VAR_TYPES){
      if (colToken.startsWith(vtype)){
        return vtype
      }
    }
    return "str" //if we can't pin it down, treat it as a string!
  }

  /**
   * parse out the end position for the data
   * @param colToken e.g. %12s
   * @return the integer position stop e.g. 12
   */
  def parseEnd(colToken:String): Integer ={
    if(colToken.startsWith("%")){
      val strippedToken = colToken.replaceAll("[^0-9]","")
      //println(strippedToken)
      return new Integer(strippedToken)
    }
    return columnStart //we don't know what to do, so just return no space for open data
  }

  /**
   * parses the description of the variable out of the line (basically the bit between the "quotes")
   * @param line
   * @return
   */
  def parseDescription(line:String): String ={
    val stack = new Stack[Int]
    //find the positions of " in the string
    for (i <- 0 to line.length()-1){
      if(line.charAt(i) == '\"'){
        stack.push(i)
      }
    }
    if(stack.size == 2){
      val j = stack.pop()
      val i = stack.pop()
      return line.substring(i+1,j)
    }
    else
      return "" //no description -- unparsable
  }

  /**
   * parses a metadata object out of the raw text
   * @param line
   */
  def parse(line: String): Unit = {
    var tokensParsed = 0
    description = parseDescription(line)
    val tokens = line.split(" ")
    for (token <- tokens) {
      if (token.size > 0) {
        //println(token)
        if(token.contains("_column")){
          columnStart = parseStart(token)
          tokensParsed += 1
        }else if (tokensParsed == 1){
          datatype = parseDatatype(token)
          tokensParsed += 1
        }else if(tokensParsed == 2){
          name = token
          tokensParsed += 1
        }else if(tokensParsed == 3){
          columnEnd = columnStart + parseEnd(token)
          tokensParsed += 1
        }
      }
    }
    return this;
  }

}


