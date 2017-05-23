package cn.sibat.bus

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class TripTest(carId: String, route: String, direct: String, firstSeqIndex: Int, ld: Double, nextSeqIndex: Int, rd: Double, tripId: Int)

/**
  * 公交到站测试类
  * Created by kong on 2017/5/2.
  */
object StationDataTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/path/to/my").appName("StationDataTest").master("local[*]").getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    //    val bStation = spark.sparkContext.broadcast(spark.read.textFile("D:/testData/公交处/lineInfo.csv").map { str =>
    //      val Array(route, direct, stationId, stationName, stationSeqId, stationLat, stationLon) = str.split(",")
    //      new StationData(route, direct, stationId, stationName, stationSeqId.toInt, stationLon.toDouble, stationLat.toDouble)
    //    }.collect())

    val mapStation = spark.read.textFile("D:/testData/公交处/lineInfo.csv").map { str =>
      val Array(route, direct, stationId, stationName, stationSeqId, stationLat, stationLon) = str.split(",")
      StationData(route, direct, stationId, stationName, stationSeqId.toInt, stationLon.toDouble, stationLat.toDouble)
    }.collect().groupBy(sd => sd.route + "," + sd.direct)

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
    //    val data = spark.read.parquet("D:/testData/公交处/confirmParquet")
    //    data.filter(col("route") === "B7114").rdd.repartition(1).saveAsTextFile("D:/testData/公交处/B7114ForTrip")
    //    data.cache()
    //    data.filter(col("carId") === lit("��BJ7547")).rdd.repartition(1).saveAsTextFile("D:/testData/公交处/BJ7547ForTrip")
    //    val group = data.groupBy(col("carId"), col("route"), col("direct"))
    //    group.max("firstSeqIndex", "ld", "nextSeqIndex", "rd", "tripId").filter(col("max(ld)") > lit(2000.0)).show()
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
    //val colLength = udf{(route:String)=>route.length}
    //busDataCleanUtils.dataFormat().data.select("route").withColumn("length",colLength(col("route"))).distinct().filter(col("length") =!= 5).show()

    //上下行差
    //    spark.read.option("inferSchema", true).option("header", false).csv("D:/testData/公交处/lineInfo.csv").groupBy("_c0", "_c1").count().select("_c0", "count")
    //      .rdd
    //      .groupBy(r => r.getString(r.fieldIndex("_c0"))).map { t =>
    //      val arr = t._2.toArray.map(r=> r.getLong(r.fieldIndex("count")))
    //      var result = 0L
    //      if (arr.length>1){
    //        result = arr.max - arr.min
    //      }
    //      (t._1,result)
    //    }.filter(_._2>2).foreach(println)

    //��BJ7547效果,局部down��BC0980
    //spark.read.textFile("D:/testData/公交处/toStation5").rdd.filter(str => str.contains("��B90036")).repartition(1).saveAsTextFile("D:/testData/公交处/B90036ToStation")

    //多路线筛选
//    val collect = spark.read.textFile("D:/testData/公交处/BJ7547ToStation").collect()
//    var count = 0
//    var start = 0
//    val firstDirect = new ArrayBuffer[String]()
//    val lonLat = new ArrayBuffer[String]()
//    var resultArr = new ArrayBuffer[String]()
//    collect.foreach { str =>
//      val split = str.split(",")
//      val carId = split(3)
//      val oldLength = 16
//      val struct = 6
//      val length = (split.length - oldLength) / struct
//      val many = (0 until length).map(i => TripTest(carId, split(oldLength + i * 6), split(oldLength + 1 + i * struct), split(oldLength + 2 + i * struct).toInt, split(oldLength + 3 + i * struct).toDouble, split(oldLength + 4 + i * struct).toInt, split(oldLength + 5 + i * struct).toDouble, 0))
//      if (count == 0) {
//        for (i <- 0 until length) {
//            firstDirect += many(i).direct + "," + i
//        }
//      } else if (!firstDirect.indices.forall(i => firstDirect(i).split(",")(0).equals(many(firstDirect(i).split(",")(1).toInt).direct))) {
//        var trueI = 0
//        val gpsPoint = FrechetUtils.lonLat2Point(lonLat.distinct.toArray)
//        var minCost = Double.MaxValue
//        val middle = firstDirect.toArray
//        firstDirect.clear()
//        for (i <- 0 until length) {
//          val line = mapStation.getOrElse(many(i).route + "," + middle(i).split(",")(0), Array()).map(sd => sd.stationLon + "," + sd.stationLat)
//          val linePoint = FrechetUtils.lonLat2Point(line)
//          val frechet = FrechetUtils.compareGesture(linePoint, gpsPoint)
//          println(frechet,many(i).route + "," + middle(i).split(",")(0))
//          if (frechet < minCost) {
//            minCost = frechet
//            trueI = i
//          }
//          firstDirect += many(i).direct + "," + i
//        }
//        resultArr ++= collect.slice(start, count).map { str =>
//          val split = str.split(",")
//          val f = (0 until 16).map(split(_)).mkString(",")
//          val s = (0 until 6).map(i => split(16 + i + trueI * 6)).mkString(",")
//          f + "," + s
//        }
//        lonLat.clear()
//        println(start)
//        start = count
//      }
//      lonLat += split(8) + "," + split(9)
//      count += 1
//    }
//    spark.sparkContext.parallelize(resultArr, 1).saveAsTextFile("D:/testData/公交处/BJ7547ToRight")

    val collect = spark.read.textFile("D:/testData/公交处/BJ7547ToStation").collect()
    //中间方向异常点纠正
    val firstTrip = new ArrayBuffer[TripTest]()
    var updateStart = 0
    var updateEnd = 0
    var count = 0
    collect.foreach { str =>
      val split = str.split(",")
      val many = toArrTrip(split)
      var temp = true
      if (many.forall(_.direct.contains("Or"))){
        if (temp) {
          updateStart = count
          temp = false
        }
      }else{
        if (!temp)
          updateEnd = count-1
        collect.slice(updateStart,updateEnd).map { s=>
          val split = s.split(",")
          val trip = toArrTrip(split)
          for (i <- many.indices) {
            if (firstTrip(i).firstSeqIndex <= many(i).firstSeqIndex) {
              trip.update(i,trip(i).copy(direct = trip(i).direct.split("Or")(0)))
            }else if (firstTrip(i).firstSeqIndex > many(i).firstSeqIndex){
              trip.update(i,trip(i).copy(direct = trip(i).direct.split("Or")(1)))
            }
          }
          ""
        }
        firstTrip ++= many
      }
      count += 1
    }
  }

  def toArrTrip(split:Array[String]): Array[TripTest] ={
    val carId = split(3)
    val oldLength = 16
    val struct = 6
    val length = (split.length - oldLength) / struct
    (0 until length).map(i => TripTest(carId, split(oldLength + i * 6), split(oldLength + 1 + i * struct), split(oldLength + 2 + i * struct).toInt, split(oldLength + 3 + i * struct).toDouble, split(oldLength + 4 + i * struct).toInt, split(oldLength + 5 + i * struct).toDouble, 0)).toArray
  }
}
