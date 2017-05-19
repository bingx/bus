package cn.sibat.taxi

import org.apache.spark.sql.SparkSession

/**测试TaxiOD的方法
  * Created by Lhh on 2017/5/16
  */
object TaxiODTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TaxiOD").config("spark.sql.warehouse.dir","file:///e:/Git/bus/my").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    //处理出租车GPS数据
    val data1 = spark.read.textFile("F:\\重要数据\\Taxi\\GPS_2016_09_24").map(
      line => line.replaceAll(",,",",null,")
    )
    val TaxiDataCleanUtils = new TaxiDataCleanUtils(data1.toDF())
    val taxiData = TaxiDataCleanUtils.dataFormat().errorsPoint().ISOFormat()
//    //处理出租车打表数据
//    val data2 = spark.read.textFile("F:\\重要数据\\TaxiDeal\\TAXIMETERS_DEAL_2016_09_24_NEW").map(
//      line => line.replaceAll(",,",",null,")
//    )
//    //加载出租车类型表
//    val carStaticDate = spark.read.textFile("F:\\重要数据\\TaxiDeal\\20170509").map(str => {
//      val Array(carId, color, company) = str.split(",")
//      new TaxiStatic(carId,color,company)
////    })
//    val TaxiDealCleanUtils = new TaxiDealCleanUtils(data2.toDF())
//    TaxiDealCleanUtils.dataFormat().distinct().getField(carStaticDate)
    val taxiDeal = spark.read.textFile("F:\\重要数据\\TaxiDeal\\result").map(row => {
      var Array(carId,date,upTime,downTime,singlePrice,runningDistance,time,
      sumPrice,emptyDistance,color,company) = row.split(",")
      carId = carId.replace("[","")
      new TaxiDealClean(carId,date,upTime,downTime,singlePrice.toDouble,runningDistance.toDouble,time,
        sumPrice.toDouble,emptyDistance,color,company)
    })
    val taxiFunc = new TaxiFunc(taxiData);
    taxiFunc.OD(taxiDeal)
  }
}
