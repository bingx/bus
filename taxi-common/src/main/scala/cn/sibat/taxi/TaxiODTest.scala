package cn.sibat.taxi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**测试TaxiOD的方法
  * Created by Lhh on 2017/5/16
  */
object TaxiODTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TaxiOD").config("spark.sql.warehouse.dir","file:///e:/Git/bus/my").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    //处理出租车GPS数据
    val data1 = spark.read.textFile("D:/testData/公交处/data/GPS_2016_09_26").map(
      line => line.replaceAll(",,",",null,")
    )
    //加载出租车类型表
    val carStaticDate = spark.read.textFile("D:/testData/公交处/data/20160927").map(str => {
      val Array(carId, color, company) = str.split(",")
      new TaxiStatic(carId,color,company)
    })

    val TaxiDataCleanUtils = new TaxiDataCleanUtils(data1.toDF())
    val taxiData = TaxiDataCleanUtils.dataFormat().errorsPoint().ISOFormat()
    //处理出租车打表数据
    val data2 = spark.read.textFile("D:/testData/公交处/data/TAXIMETERS_DEAL_2016_09_26_NEW").map(
      line => line.replaceAll(",,",",null,")
    )

    val TaxiDealCleanUtils = new TaxiDealCleanUtils(data2.toDF())
    val taxiDealClean = TaxiDealCleanUtils.dataFormat().distinct().ISOFormatRe().getField(carStaticDate)
    val bTaxiDeal = spark.sparkContext.broadcast(taxiDealClean.collect())

    val time2Date = udf((upTime:String)=> upTime.split("T")(0))
    val time2DateAndHour = udf((downTime:String) => {
      val split = downTime.split("T")
      split(0)+","+split(1).split(":")(0)
    })

    val group = taxiDealClean.toDF().groupBy(col("color"),time2Date(col("upTime")))
    group.count().show()

    group.sum("emptyDistance","runningDistance").show()
    taxiDealClean.toDF().groupBy(col("color"),time2DateAndHour(col("downTime"))).avg("sumPrice").show()

//    val taxiDeal = spark.read.textFile("F:\\重要数据\\TaxiDeal\\result").map(row => {
//      var Array(carId,date,upTime,downTime,singlePrice,runningDistance,time,
//      sumPrice,emptyDistance,color,company) = row.split(",")
//      carId = carId.replace("[","")
//      new TaxiDealClean(carId,date,upTime,downTime,singlePrice.toDouble,runningDistance.toDouble,time,
//        sumPrice.toDouble,emptyDistance,color,company)
//    })
//    val taxiFunc = new TaxiFunc(taxiData)
//    taxiFunc.OD(bTaxiDeal).show()
  }
}
