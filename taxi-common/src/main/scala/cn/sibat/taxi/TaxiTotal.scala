package cn.sibat.taxi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by kong on 2017/7/1.
  */
object TaxiTotal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TaxiOD").master("spark://hadoop-1:7077").getOrCreate()

    import spark.implicits._

    //加载出租车类型表
    val carStaticDate = spark.read.textFile("/user/kongshaohong/taxiStatic/20160927").map(str => {
      val Array(carId, color, company) = str.split(",")
      TaxiStatic(carId,color,company)
    }).collect()
    val bCarStaticDate = spark.sparkContext.broadcast(carStaticDate)

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
    })
    //    val time2DateAndHour = udf((downTime:String) => {
    //      val split = downTime.split(" ")
    //      split(0)+","+split(1).split(":")(0)
    //    })

//    val group = taxiDealClean.toDF().groupBy(col("color"),col("date"))
//    group.count().rdd.saveAsTextFile("/user/kongshaohong/passengerTotal")
//    group.sum("emptyDistance","runningDistance").rdd.saveAsTextFile("/user/kongshaohong/disTotal")
    //taxiDealClean.toDF().groupBy(col("color"),time2DateAndHour(col("downTime"))).avg("sumPrice").show()
    taxiDealClean.groupByKey(tdc => tdc.color+","+tdc.date).flatMapGroups{(s,it)=>
      val map = new mutable.HashMap[String,TaxiODTest]()
      val result = new ArrayBuffer[String]()
      it.foreach(tdc=>{
        val key = tdc.upTime.split(" ")(1).split(":")(0)
        val tot = map.getOrElse(key,new TaxiODTest(0.0,0.0,0.0,new mutable.HashSet[String]()))
        val emptyDis = tot.emptyTotal + tdc.emptyDistance
        val runDis = tot.runTotal + tdc.runningDistance
        val priceTotal = tot.priceTotal + tdc.sumPrice
        tot.set += tdc.carId
        map.put(key,tot.copy(emptyTotal = emptyDis,runTotal = runDis,priceTotal = priceTotal,set = tot.set))
      })
      var tot = new TaxiODTest(0.0,0.0,0.0,new mutable.HashSet[String]())
      map.foreach(tuple=>{
        tot = tot.copy(emptyTotal = tot.emptyTotal+tuple._2.emptyTotal,runTotal = tot.runTotal+tuple._2.runTotal
          ,priceTotal = tot.priceTotal+tuple._2.priceTotal,set = tot.set.++=(tuple._2.set))
        result += s.split(",")(1)+","+tuple._1+","+tuple._2.toString
      })
      result += s.split(",")(1)+",all,"+tot.toString
      result
    }.repartition(20).rdd.saveAsTextFile("/user/kongshaohong/all")
  }
}
