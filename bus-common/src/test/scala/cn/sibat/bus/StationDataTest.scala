package cn.sibat.bus

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

case class Trip(carId: String, route: String, direct: String, firstSeqIndex: Int, ld: Double, nextSeqIndex: Int, rd: Double, tripId: Int)

/**
  * 公交到站测试类
  * Created by kong on 2017/5/2.
  */
object StationDataTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/path/to/my").appName("StationDataTest").master("local[*]").getOrCreate()
    import spark.implicits._
    val bStation = spark.sparkContext.broadcast(spark.read.textFile("D:/testData/公交处/lineInfo.csv").map { str =>
      val Array(route, direct, stationId, stationName, stationSeqId, stationLat, stationLon) = str.split(",")
      new StationData(route, direct, stationId, stationName, stationSeqId.toInt, stationLon.toDouble, stationLat.toDouble)
    }.collect())

    //查看某辆车
    //    val filter_1 = udf{(carId:String)=>
    //      carId.contains("��BJ7547")
    //    }
    //    spark.read.textFile("D:/testData/公交处/toStation2").filter(filter_1(col("value"))).rdd.repartition(1).saveAsTextFile("D:/testData/公交处/BJ7547")

    //线路确认
    //    spark.read.textFile("D:/testData/公交处/toStation2").flatMap { str =>
    //      var result = new ArrayBuffer[Trip]()
    //      val split = str.split(",")
    //      val carId = split(3)
    //      val oldLength = 16
    //      val struct = 6
    //      val length = (split.length - oldLength) / struct
    //      result = result ++ (0 until length).map(i => Trip(carId, split(oldLength + i * 6), split(oldLength + 1 + i * struct), split(oldLength + 2 + i * struct).toInt, split(oldLength + 3 + i * struct).toDouble, split(oldLength + 4 + i * struct).toInt, split(oldLength + 5 + i * struct).toDouble, 0))
    //      result
    //    }.groupByKey(t => t.carId + "," + t.route).flatMapGroups { (str, it) =>
    //      val stationMap = bStation.value.groupBy(sd => sd.route + "," + sd.direct)
    //      var tripId = 1
    //      var firstDirect: Trip = null
    //      var count = 0
    //      var direct = "up"
    //      it.map { t =>
    //        val stationSize = stationMap.getOrElse(t.route + "," + t.direct, Array()).length
    //        if (count == 0) {
    //          firstDirect = t
    //          if (math.abs(t.nextSeqIndex - stationSize) <= math.abs(t.nextSeqIndex - 1)) {
    //            direct = "down"
    //          }
    //        } else {
    //          if (direct.equals("up")) {
    //            if (t.nextSeqIndex < firstDirect.nextSeqIndex && math.abs(stationSize - t.nextSeqIndex) < 3) {
    //              tripId += 1
    //              direct = "down"
    //            }
    //          } else {
    //            if (t.nextSeqIndex > firstDirect.nextSeqIndex && math.abs(t.firstSeqIndex - 1) < 2) {
    //              tripId += 1
    //              direct = "up"
    //            }
    //          }
    //          firstDirect = t
    //        }
    //        count = 1
    //        t.copy(t.carId, t.route, direct, t.firstSeqIndex, t.ld, t.nextSeqIndex, t.rd, tripId)
    //      }
    //    }.write.parquet("D:/testData/公交处/confirmParquet")
    //.rdd.saveAsTextFile("D:/testData/公交处/confirm")

    //分趟验证
    val data = spark.read.parquet("D:/testData/公交处/confirmParquet")
    data.cache()
    val group = data.groupBy(col("carId"), col("route"), col("direct"))
    group.max("firstSeqIndex", "ld", "nextSeqIndex", "rd", "tripId").filter(col("max(ld)") > lit(2000.0)).show()
    //group.min("firstSeqIndex", "ld", "nextSeqIndex", "rd", "tripId").show()
    //group.avg("firstSeqIndex", "ld", "nextSeqIndex", "rd", "tripId").show()


    //线路匹配
    //    val data = spark.read.textFile("D:/testData/公交处/data/2016-12-01/*/*")
    //    val busDataCleanUtils = new BusDataCleanUtils(data.toDF())
    //    val filter = busDataCleanUtils.dataFormat().zeroPoint().filterStatus() //.data.filter(col("carId") === lit("��BCK127")) //��B89863
    //    val roadInformation = new RoadInformation(filter)
    //
    //    roadInformation.toStation(bStation)

    //查看某条线路
    //    val time2date = udf { (upTime: String) =>
    //      upTime.split("T")(0)
    //    }
    //    filter.data.select(col("carId"), time2date(col("upTime")).as("upTime"), col("route")).distinct().rdd
    //      .groupBy(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime"))).filter(_._2.size>1).foreach(s=>println(s._1))
    //roadInformation.routeConfirm(bStation)

    //上传线路异常检测
    //    val colLength = udf{(route:String)=>route.length}
    //    busDataCleanUtils.dataFormat().data.select("route").withColumn("length",colLength(col("route"))).distinct().filter(col("length") =!= 5).show()

    //    val min2 = Array(Double.MaxValue, Double.MaxValue)
    //    var array = new ArrayBuffer[String]()
    //    var count = 0
    //    spark.read.option("inferSchema", true).option("header", false).csv("D:/testData/公交处/line20170228.csv").collect().foreach { row =>
    //      val t_lon = 114.082939
    //      val t_lat = 22.732796
    //      val lon = row.getDouble(row.fieldIndex("_c6"))
    //      val lat = row.getDouble(row.fieldIndex("_c5"))
    //      val dis = LocationUtil.distance(lon, lat, t_lon, t_lat)
    ////      if (math.abs(t_lat - lat) < 10)
    ////        println(math.abs(t_lat - lat), math.abs(t_lon - lon))
    //      if (min2.max > dis) {
    //        println(lat, lon,row.getString(row.fieldIndex("_c0")))
    //        min2(min2.indexOf(min2.max)) = dis
    //        if (array.size < 2)
    //          array.+=(lon+","+lat)
    //        else
    //          array = array.tail.+=(lon+","+lat)
    //      }
    //      count+=1
    //    }
    //    if (array.size > 1) {
    //      val pd = LocationUtil.distance(array(0).split(",")(0).toDouble, array(0).split(",")(1).toDouble, array(1).split(",")(0).toDouble, array(1).split(",")(1).toDouble)
    //      println(pd)
    //      if (min2.sum < 1.2 * pd) {
    //        println("+++++++")
    //      }
    //    }
    //    println(count)
    //    println(min2.mkString(";"))
  }
}
