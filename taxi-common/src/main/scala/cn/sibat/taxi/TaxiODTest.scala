package cn.sibat.taxi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet}

case class TaxiODTest(emptyTotal: Double, runTotal: Double, priceTotal: Double, set: HashSet[String]) {
  override def toString: String = emptyTotal / set.size + "," + runTotal / set.size + "," + priceTotal / set.size
}

/** 测试TaxiOD的方法
  * Created by Lhh on 2017/5/16
  */
object TaxiODTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TaxiOD").master("spark://hadoop-1:7077").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    //处理出租车GPS数据
    val data1 = spark.read.textFile("/user/kongshaohong/taxiGPS/GPS_2016_09_26").map(
      line => line.replaceAll(",,", ",null,")
    )
    //加载出租车类型表
    val carStaticDate = spark.read.textFile("/user/kongshaohong/taxiStatic/20160927").map(str => {
      val Array(carId, color, company) = str.split(",")
      new TaxiStatic(carId, color, company)
    }).collect()
    val bCarStaticDate = spark.sparkContext.broadcast(carStaticDate)

    val TaxiDataCleanUtils = new TaxiDataCleanUtils(data1.toDF())
    val taxiData = TaxiDataCleanUtils.dataFormat().errorsPoint()
    //处理出租车打表数据
    //    val data2 = spark.read.textFile("D:/testData/公交处/data/TAXIMETERS_DEAL_2016_09_26_NEW").map(
    //      line => line.replaceAll(",,",",null,")
    //    )
    //
    //    val TaxiDealCleanUtils = new TaxiDealCleanUtils(data2.toDF())
    //    val taxiDealClean = TaxiDealCleanUtils.dataFormat().distinct().ISOFormatRe().getField(carStaticDate)

    val taxiDealClean = spark.read.textFile("/user/kongshaohong/taxiDeal/*").map(s=>{
      val split = s.split(",")
      val date = split(3).split(" ")(0)
      val TS = bCarStaticDate.value.filter(ts=> ts.carId.equals(split(2)))
      var color = "null"
      var company = split(11)
      if (TS.length>0) {
        color = TS(0).color
        company = TS(0).company
      }
      TaxiDealClean(split(2),date,split(3),split(4),split(5).toDouble,split(6).toDouble,split(7),split(8).toDouble,split(9).toDouble,color,company)
    }).toDF()
    val bTaxiDeal = spark.sparkContext.broadcast(taxiDealClean.collect())

    val taxiFunc = new TaxiFunc(taxiData)
    taxiFunc.OD(bTaxiDeal).rdd.saveAsTextFile("/user/kongshaohong/taxiOD")
  }
}
