package cn.sibat.bus

import org.apache.spark.sql.SparkSession

/**
  * 公交到站测试类
  * Created by kong on 2017/5/2.
  */
object StationDataTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/path/to/my").appName("StationDataTest").master("local[*]").getOrCreate()
    import spark.implicits._
    val bStation = spark.sparkContext.broadcast(spark.read.textFile("D:/testData/公交处/line20170228.csv").map { str =>
      val Array(route, direct, stationId, stationName, stationSeqId, stationLat, stationLon) = str.split(",")
      new StationData(route, direct, stationId, stationName, stationSeqId.toLong, stationLon.toDouble, stationLat.toDouble)
    }.collect())

    val data = spark.read.textFile("D:/testData/公交处/data/2016-12-01/00.gz/*")
    val busDataCleanUtils = new BusDataCleanUtils(data.toDF())
    val filter = busDataCleanUtils.dataFormat().zeroPoint().filterStatus()
    val roadInformation = new RoadInformation(filter)
    roadInformation.toStation(bStation)
//    val min2 = Array(Double.MaxValue, Double.MaxValue)
//    spark.read.option("inferSchema", true).option("header", false).csv("D:/testData/公交处/line20170228.csv").collect().foreach { row =>
//      val t_lon = 114.104103
//      val t_lat = 22.656336
//      val lon = row.getDouble(row.fieldIndex("_c6"))
//      val lat = row.getDouble(row.fieldIndex("_c5"))
//      val dis = LocationUtil.distance(lon, lat, t_lon, t_lat)
//      if (math.abs(t_lat - lat) < 10)
//        println(math.abs(t_lat - lat), math.abs(t_lon - lon))
//      if (min2.max > dis) {
//        //println(lat, lon)
//        min2(min2.indexOf(min2.max)) = dis
//      }
//    }
//    println(min2.mkString(";"))
  }
}
