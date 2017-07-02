package cn.sibat.taxi

import java.net.URLDecoder
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/7/2.
  */
object ReadTaxiGPSToHDFS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ReadTaxiGPSToHDFS").getOrCreate()
    import spark.implicits._
    // /parastor/backup/data/all_data/TAXIGPS/gpsfile11-12/track/2016/11/01
    val arg = args(0)
    val argData = args(1)
    val list = "20160913\n20160920\n20160927\n20161004\n20161018\n20161025\n20161101\n20161108\n20161129\n20161206\n20161213\n20161220\n20161227\n20170103\n20170110\n20170117\n20170124\n20170207\n20170214\n20170221\n20170228\n20170307\n20170321\n20170328\n20170411\n20170418\n20170425\n20170502\n20170509\n20170516\n20170523\n20170530\n20170606\n20170613\n20170620\n20170627".split("\n")
    var min = list(0)
    var minNum = Int.MaxValue
    list.foreach(s=>
      if (minNum > s.toInt - argData.toInt){
        min = s
        minNum = s.toInt - argData.toInt
      }
    )
    //加载出租车类型表
    val carStaticDate = spark.read.textFile("/user/kongshaohong/taxiStatic/"+min).map(str => {
      val Array(carId, color, company) = str.split(",")
      TaxiStatic(carId, color, company)
    }).collect()
    val bCarStaticDate = spark.sparkContext.broadcast(carStaticDate)

    val carId = spark.read.textFile("/user/kongshaohong/carId").collect()
    val bCarId = spark.sparkContext.broadcast(carId)

    spark.sparkContext.wholeTextFiles("file:///parastor/backup/data/all_data/TAXIGPS"+arg+"/*.txt").filter(t=>{
      val split = URLDecoder.decode(t._1,"GBK").split("_")
      val carId = split(split.length-1).replace(".txt","")
      bCarId.value.contains(carId)
    }).flatMap(tuple=>{
      val carId = URLDecoder.decode(tuple._1,"GBK").split("_")(1).replace(".txt","")
      val sdf = new SimpleDateFormat("yyyyMMdd/HHmmss")
      val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      tuple._2.split("\n").map(s=> {
        val split = s.replace("::",":null:").split(":")
        val time = sdf1.format(sdf.parse(split(2)))
        val cars = bCarStaticDate.value.filter(ts => ts.carId.equals(carId.trim))
        var color = "红色"
        var company = "null"
        if (cars.length > 0){
          color = cars(0).color
          company = cars(0).company
        }
        //车牌号，经度，纬度，上传时间，设备号，速度，方向，定位状态，未知，SIM卡号，车辆状态，车辆颜色
        TaxiData(carId,split(7).toDouble/600000,split(8).toDouble/600000,time,company,split(3),split(4),split(12),split(10),split(13),split(5),"color")
      })
    }).toDF.write.parquet("/user/kongshaohong/taxiGPS/"+argData)
    //val gps = spark.read.text("file:///parastor/backup/data/all_data/"+args+"*.txt")
    //val bCarId = spark.sparkContext.broadcast(spark.read.textFile("/user/kongshaohong/carId").collect())
  }
}
