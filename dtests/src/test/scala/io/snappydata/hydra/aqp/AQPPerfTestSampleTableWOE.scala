package io.snappydata.hydra.aqp

import java.io.PrintWriter

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappyContext, SnappySQLJob}


/**This test will let us know the cost of sampling
 * Created by supriya on 4/10/16.
 */
object AQPPerfTestSampleTableWOE extends SnappySQLJob {

  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    val numIter = jobConfig.getString("numIter").toInt
    val queryFile :String = jobConfig.getString("queryFile")
    val sampleDataLocation : String = jobConfig.getString("sampleDataLocation")
    val df = snc.sql("SELECT * from airline_sample")
    df.write.csv(sampleDataLocation)
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    val execTimeArray = new Array[Double](queryArray.length)

    def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
    val props = Map[String, String]()
    val pw = new PrintWriter("AQPPerfTestSampleTableWOE.out")


    val skipTill = jobConfig.getString("skipTill").toInt

    def createSampleTableWOE():Unit={

      storeSampleTableValue()

      //Use the saved file to crate a sample table ,without actually re-sampling it,because the data in the sample.csv is already sampled.
      val df = snc.read.load(sampleDataLocation).toDF("YEAR_","Month_", "DayOfMonth",
        "DayOfWeek" ,"UniqueCarrier","TailNum" ,"FlightNum","Origin" ,"Dest","CRSDepTime","DepTime" ,"DepDelay","TaxiOut",
        "TaxiIn","CRSArrTime","ArrTime" ,"ArrDelay","Cancelled","CancellationCode","Diverted","CRSElapsedTime",
        "ActualElapsedTime","AirTime","Distance","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay",
        "LateAircraftDelay","ArrDelaySlot", "SNAPPY_SAMPLER_WEIGHTAGE")
      snc.createTable("sampleTable_WOE", "column", df.schema, Map("buckets" -> "8"))
      df.write.insertInto("sampleTable_WOE")
    }

    //Save the already created sampletable data as a parquet
    def storeSampleTableValue():Unit={
      val df = snc.sql("SELECT * from airline_sample")
      df.write.parquet(sampleDataLocation)

    }

    Try {

      AQPPerfTestUtil.runPerftest(numIter,snc,pw,queryArray,skipTill,execTimeArray)

     } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/AQPPerfResults.out"
      case Failure(e) => pw.close();
        throw e;
    }

  }

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()


}
