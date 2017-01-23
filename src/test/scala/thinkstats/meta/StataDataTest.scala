package thinkstats.meta

import org.apache.spark.sql.types.{StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import spire.syntax.field


/**
 * Created by m102417 on 12/28/16.
 */

@RunWith(classOf[JUnitRunner])
class StataDataTest extends FlatSpec with Matchers {

  val metaFile = "src/test/resources/2002FemPreg.dct"
  val dataFile = "src/test/resources/2002FemPreg.dat.gz"

  "Stata data" should "parse the entire metadata file properly" in {
    var meta = new StataData(metaFile)
    meta.metadata.size should be (243)

  }

  "Stata data" should "parse a line from the data file properly" in {
    var meta = new StataData(metaFile)
    val dataLine = "           1 1     6 1     11093 1084     9 039 9   0  1 813             1093 13837                        1                5                                                                        116610931166 9201093                111             3    1   12  11         5391 110933316108432411211     2995 1212 69544441116122222 2 2224693215    000000000000000000000000000000000000003410.38939935294273869.3496019830486 6448.2711117047512 91231"
    val json = meta.dataLine2JSON(dataLine)
    //println(json)
    json should include ("{\"rcurpreg\":2,\"cmfstprg\":1093,\"oldwantr_i\":0,\"race_i\":0,\"religion_i\":0,\"wksgest\":39,\"hieduc_i\":0,\"cmintvw\":1231,\"fmarital_i\":0,\"wthpart1\":1,\"insuranc\":2,\"insuranc_i\":0,\"agepreg_i\":0,\"race\":2,\"birthord_i\":0,\"religion\":2,\"datend\":1093,\"cmbirth\":695,\"oldwantr\":1,\"cmlstprg\":1166,\"rmarital_i\":0,\"agescrn\":44,\"pregnum_i\":0,\"prglngth_i\":0,\"ager_i\":0,\"fmarcon5\":1,\"wantresp_i\":0,\"metro\":1,\"caseid\":\"1\",\"metro_i\":0,\"wantpart\":2,\"rmarout6_i\":0,\"anyusint\":5,\"poverty\":469,\"matchfound\":1,\"kidage\":138,\"birthwgt_oz\":13,\"datecon\":1084,\"fmarout5_i\":0,\"hpagelb\":37,\"educat_i\":0,\"rcurpreg_i\":0,\"hispanic_i\":0,\"timokhp\":2,\"whystopd\":1,\"agecon_i\":0,\"parity_i\":0,\"pregnum\":2,\"cmlastlb\":1166,\"outcome\":1,\"anynurse\":5,\"birthord\":1,\"cmprgend\":1093,\"sest\":9,\"hisprace_i\":0,\"laborfor\":3,\"birthwgt_lb\":8,\"pregordr\":1,\"hpwnold\":1,\"bpa_bdscheck1\":0,\"cmbabdob\":1093,\"pregend1\":6,\"datecon_i\":0,\"adj_mod_basewgt\":3869.3496019830486,\"prgoutcome\":1,\"cmintfin\":1093,\"bfeedwks_i\":0,\"ager\":44,\"fmarcon5_i\":0,\"whentell\":1,\"secu_p\":2,\"timingok\":3,\"gestasun_w\":0,\"finalwgt\":6448.271111704751,\"lbw1_i\":0,\"datend_i\":0,\"agepreg\":3316,\"pubassis_i\":0,\"pncarewk_i\":0,\"agecon\":3241,\"outcome_i\":0,\"hispanic\":2,\"tellfath\":1,\"evuseint\":1,\"rmarout6\":1,\"hieduc\":12,\"parity\":2,\"educat\":16,\"oldwantp\":2,\"stopduse\":1,\"maternlv_i\":0,\"pubassis\":2,\"mosgest\":9,\"nbrnaliv\":1,\"cmprgbeg\":1084,\"oldwantp_i\":0,\"bfeedwks\":995,\"pmarpreg\":2,\"basewgt\":3410.3893993529427,\"learnprg_i\":0,\"cmintstr\":920,\"paydeliv_i\":0,\"babysex\":1,\"fmarout5\":1,\"pmarpreg_i\":0,\"prglngth\":39,\"poverty_i\":0,\"fmarital\":1,\"brnout\":5,\"gestasun_m\":9,\"rmarital\":1,\"wantpart_i\":0,\"wantresp\":1,\"laborfor_i\":0,\"lbw1\":2,\"hisprace\":2}")

  }

  "Stata data" should "be able to convert the raw file into a schema object for use in spark encoders" in {
    val example = "src/test/resources/example.dct" //this is an example dct, shortened down so the test is easier to write
    val meta = new StataData(example)
    var i = 0
    for ( field <- meta.schema().iterator ) {
      if(i==0){
        field.name should be ("A")
        field.dataType.toString() should be ("StringType")
      }
      if(i==1){
        field.name should be ("B")
        field.dataType.toString() should be ("ByteType")
      }
      if(i==2){
        field.name should be ("C")
        field.dataType.toString() should be ("IntegerType")
      }
      if(i==3){
        field.name should be ("D")
        field.dataType.toString() should be ("FloatType")
      }
      if(i==4){
        field.name should be ("E")
        field.dataType.toString() should be ("DoubleType")
      }
      i = i + 1
    }

  }

}
