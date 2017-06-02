package cn.sibat.bus

import java.io.File

import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
import org.apache.spark.examples.h2o.{Airlines, WeatherParse}
import org.apache.spark.h2o.{H2OConf, H2OContext, H2OFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.examples.h2o.AirlinesWithWeatherDemo2.residualPlotRCode

import scala.tools.nsc.Settings

/**
  * sparkling-water
  * Created by kong on 2017/5/27.
  */
object SparklingWaterDemo {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparklingWaterDemo").master("local[*]").getOrCreate()
    spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")
    val h2oContext = H2OContext.getOrCreate(spark)

    import spark.implicits._
    val weatheDataFile = "D:/download/sparkling-water-2.0.0/examples/smalldata/Chicago_Ohare_International_Airport.csv"
    val wrawdata = spark.sparkContext.textFile(weatheDataFile,3).cache()
    val weatherTable = wrawdata.map(_.split(",")).map(row=>WeatherParse(row)).filter(!_.isWrongRow())

    val dataFile = "D:/download/sparkling-water-2.0.0/examples/smalldata/allyears2k_headers.csv.gz"
    val airlinesData = new H2OFrame(new File(dataFile))

    val airlinesTable = h2oContext.asRDD[Airlines](airlinesData)
    val flightsToORD = airlinesTable.filter(f=>f.Dest.contains("ORD"))

    flightsToORD.count()

    flightsToORD.toDF.createOrReplaceTempView("FlightsToORD")
    weatherTable.toDF.createOrReplaceTempView("WeatherORD")

    val bigTable = spark.sql(
      """SELECT
        |f.Year,f.Month,f.DayofMonth,
        |f.CRSDepTime,f.CRSArrTime,f.CRSElapsedTime,
        |f.UniqueCarrier,f.FlightNum,f.TailNum,
        |f.Origin,f.Distance,
        |w.TmaxF,w.TminF,w.TmeanF,w.PrcpIn,w.SnowIn,w.CDD,w.HDD,w.GDD,
        |f.ArrDelay
        |FROM FlightsToORD f
        |JOIN WeatherORD w
        |ON f.Year=w.Year AND f.Month=w.Month AND f.DayofMonth=w.Day
      """.stripMargin)

    val bigDataFrame:H2OFrame = h2oContext.asH2OFrame(bigTable)

    for (i <- 0 to 2)
      bigDataFrame.replace(i,bigDataFrame.vec(i).toCategoricalVec)
    bigDataFrame.update()

    val dlParams = new DeepLearningParameters()
    dlParams._train = bigDataFrame.key
    dlParams._response_column = "ArrDelay"
    dlParams._epochs = 5
    dlParams._activation = Activation.RectifierWithDropout
    dlParams._hidden = Array[Int](100,100)

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel().get()

    val predictionH2OFrame = dlModel.score(h2oContext.asH2OFrame(bigTable),"predict")
    val predictionsFromModel = h2oContext.asDataFrame(predictionH2OFrame)(spark.sqlContext).collect.map(row => if (row.isNullAt(0)) Double.NaN else row(0))

    residualPlotRCode(predictionH2OFrame,"predict",bigDataFrame,"ArrDelay",h2oContext)
  }
}
