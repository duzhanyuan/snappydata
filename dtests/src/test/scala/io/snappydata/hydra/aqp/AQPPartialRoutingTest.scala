package io.snappydata.hydra.aqp

import java.io.PrintWriter

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappySQLJob, DataFrame, SaveMode, SnappyJobValid, SnappyJobValidation, SnappyContext}

/**
 * Created by supriya on 5/10/16.
 */
object AQPPartialRoutingTest extends SnappySQLJob {
  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    val numIter = jobConfig.getString("numIter").toInt
    val queryFile :String = jobConfig.getString("queryFile");
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    val execTimeArray = new Array[Double](queryArray.length)


    def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
    val props = Map[String, String]()
    val pw = new PrintWriter("AQPPartialRoutingTest.out")

     //Create an empty table with VARCHAR datatype instead of String
    val airlineDataFrame = snc.sql( s"""CREATE TABLE AIRLINE (
                 YEAR_ INTEGER NOT NULL,
                 MONTH_ INTEGER NOT NULL,
                 DayOfMonth INTEGER NOT NULL,
                 DayOfWeek INTEGER NOT NULL,
                 DepTime INTEGER,
                 CRSDepTime INTEGER,
                 ArrTime INTEGER,
                 CRSArrTime INTEGER,
                 UniqueCarrier VARCHAR(20) NOT NULL,
                 FlightNum VARCHAR(20),
                 TailNum INTEGER,
                 ActualElapsedTime INTEGER,
                 CRSElapsedTime INTEGER,
                 AirTime INTEGER,
                 ArrDelay INTEGER,
                 DepDelay INTEGER,
                 Origin VARCHAR(20),
                 Dest VARCHAR(20),
                 Distance INTEGER,
                 TaxiIn INTEGER,
                 TaxiOut INTEGER,
                 Cancelled INTEGER,
                 CancellationCode VARCHAR(20),
                 Diverted INTEGER,
                 CarrierDelay INTEGER,
                 WeatherDelay INTEGER,
                 NASDelay INTEGER,
                 SecurityDelay INTEGER,
                 LateAircraftDelay INTEGER,
                 ArrDelaySlot VARCHAR(20)
              ) PARTITION BY COLUMN (UniqueCarrier)
              BUCKETS '11'
         """)

     //Populate the AIRLINE table as row table
    airlineDataFrame.write.format("row").mode(SaveMode.Append).saveAsTable("AIRLINE")
    val skipTill = jobConfig.getString("skipTill").toInt
    Try {

      AQPPerfTestUtil.runPerftest(numIter,snc,pw,queryArray,skipTill,execTimeArray)

    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/AQPPartialRoutingTest.out"
      case Failure(e) => pw.close();
        throw e;
    }

  }

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()
}
