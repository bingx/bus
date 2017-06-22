package cn.sibat.bus

import java.text.SimpleDateFormat

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RoadInformation(busDataCleanUtils: BusDataCleanUtils) extends Serializable {

  import busDataCleanUtils.data.sparkSession.implicits._

  def joinInfo(): Unit = {
    //shp文件，进行道路匹配
    busDataCleanUtils.data.sparkSession.read.textFile("").as[TestBus]

  }

  /**
    * 多线路匹配算法
    * 主要是利用弗雷歇距离判定
    * 线路与gps点的距离越小则越相似
    * e.g 原始数据加推算内容
    * 2016-12-01T17:28:25.000Z,00,P��,��B90036,M2413,M2413,2,0,113.875015,22.584335,0.0,2016-12-01T17:28:18.000Z,41.0,140.0,41.0,0.0,M2413,down,1,1188.7146351996068,2,344.7998971948696
    * 其中M2413,down,1,1188.7146351996068,2,344.7998971948696是推算出来
    *
    * @param gps        推算距离后的gps
    * @param stationMap 站点静态数据map
    * @return
    */
  private def routeConfirm(gps: Array[String], stationMap: Map[String, Array[StationData]], oldLength: Int = 16, maybeLine: Int): Array[String] = {
    var count = 0
    var start = 0
    val firstDirect = new ArrayBuffer[String]()
    val lonLat = new ArrayBuffer[String]()
    var tripId = 0
    var resultArr = new ArrayBuffer[String]()
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    var flag = false
    gps.foreach { str =>
      val split = str.split(",")
      val many = toArrTrip(split, oldLength)
      if (count == 0) {
        for (i <- many.indices) {
          firstDirect += many(i).direct + "," + i
        }
      } else if (!firstDirect.indices.forall(i => firstDirect(i).split(",")(0).equals(many(firstDirect(i).split(",")(1).toInt).direct))) {
        var trueI = 0
        if (maybeLine > 1) {
          val gpsPoint = FrechetUtils.lonLat2Point(lonLat.distinct.toArray)
          var minCost = Double.MaxValue
          val middle = firstDirect.toArray
          firstDirect.clear()
          for (i <- many.indices) {
            val line = stationMap.getOrElse(many(i).route + "," + middle(i).split(",")(0), Array()).map(sd => sd.stationLon + "," + sd.stationLat)
            val linePoint = FrechetUtils.lonLat2Point(line)
            val frechet = FrechetUtils.compareGesture(linePoint, gpsPoint)
            if (frechet < minCost) {
              minCost = frechet
              trueI = i
            }
            firstDirect += many(i).direct + "," + i
          }
        } else {
          firstDirect.clear()
          for (i <- many.indices) {
            firstDirect += many(i).direct + "," + i
          }
        }
        val timeStart = gps(start).split(",")(11)
        val timeEnd = gps(count).split(",")(11)
        val time = (sdf.parse(timeEnd).getTime - sdf.parse(timeStart).getTime) / 1000
        //初次执行完需要更新加1，下面就得等下一轮，使趟次少1
        if (time > 20 * 60 && tripId == 0 && flag) {
          tripId += 1
        }

        resultArr ++= gps.slice(start, count).map { str =>
          val split = str.split(",")
          val f = (0 until oldLength).map(split(_)).mkString(",")
          val s = (0 until 6).map(i => split(oldLength + i + trueI * 6)).mkString(",")
          f + "," + s + "," + tripId
        }

        //不过半小时的趟次合并到满的趟次里
        if (time > 20 * 60) {
          if (flag) {
            tripId += 1
            flag = false
          }
          flag = true
        }

        lonLat.clear()
        start = count
      }
      lonLat += split(8) + "," + split(9)
      count += 1
    }
    toBusArrivalData(resultArr.toArray, stationMap)
  }

  /**
    * 中间异常点纠正
    * 规则：
    * 异常点的前一正常点1与下一正常点2，若1的站点index<=2的站点index，方向正确，Or的第一个方向
    * 若1的站点index>2的站点index，方向错误，Or的第二个方向
    *
    * @param gps 推算后的gps
    * @return 纠正后的gps数据
    */
  private def error2right(gps: Array[String]): Array[String] = {
    val firstTrip = new ArrayBuffer[Trip]()
    var updateStart = 0
    var updateEnd = 0
    var count = 0
    var temp = true
    val result = new ArrayBuffer[String]()
    gps.foreach { str =>
      val split = str.split(",")
      val many = toArrTrip(split)
      if (many.forall(_.direct.contains("Or"))) {
        if (temp) {
          updateStart = count
          temp = false
        }
      } else {
        if (!temp) {
          updateEnd = count - 1
          result ++= gps.slice(updateStart, updateEnd).map { s =>
            val split = s.split(",")
            val trip = toArrTrip(split)
            for (i <- many.indices) {
              if (firstTrip(i).firstSeqIndex <= many(i).firstSeqIndex && trip(i).direct.contains("Or")) {
                trip.update(i, trip(i).copy(direct = trip(i).direct.split("Or")(0)))
              } else if (firstTrip(i).firstSeqIndex > many(i).firstSeqIndex && trip(i).direct.contains("Or")) {
                trip.update(i, trip(i).copy(direct = trip(i).direct.split("Or")(1).toLowerCase()))
              }
            }
            (0 until 16).map(split(_)).mkString(",") + "," + trip.indices.map(trip(_).toString).mkString(",")
          }
          temp = true
        }
        result += str
        //保证只有前一条记录
        if (firstTrip.isEmpty)
          firstTrip ++= many
        else {
          firstTrip.clear()
          firstTrip ++= many
        }

      }
      count += 1
    }
    result.toArray
  }

  /**
    * 把推算内容变成结构体
    *
    * @param split     arr[String]
    * @param oldLength 默认16
    * @return
    */
  private def toArrTrip(split: Array[String], oldLength: Int = 16): Array[Trip] = {
    val carId = split(3)
    val struct = 6 //结构体默认长度
    val length = (split.length - oldLength) / struct
    (0 until length).map(i => Trip(carId, split(oldLength + i * 6), split(oldLength + 1 + i * struct), split(oldLength + 2 + i * struct).toInt, split(oldLength + 3 + i * struct).toDouble, split(oldLength + 4 + i * struct).toInt, split(oldLength + 5 + i * struct).toDouble, 0)).toArray
  }

  /**
    * 1.从备选库得出备选线路id，默认上传的线路为正确线路
    * 2.按天和车分组进行操作
    * 3.分组操作内容
    * 3.1 按时间upTime进行排序
    * 3.2 选取前两个不同位置的点，做方向确认，若识别方向为up，但是接近up的终点站200m，则方向为down，同理为up，否则为识别的方向
    * 原理 A---------------------B，AB为终点站，A作为up的初站点，down的末站点，B为末站点，down的初站点
    * ---------C--D--E------------，D为第一个点，若C为第二个点则相对A站点up的反方向，方向为down，以此类推
    * 3.3 根据线路方向，把车所在最近站点位置推算出来添加在数据后面，线路，方向，前一站点index，前一站点距离，下一站点index，下一站点距离（多线路加多个）
    * 3.4 达到线路的末位置，则切换方向，中点偏离点标记为正常方向+Or+异常方向，可能是漂移也可能是没到站点就切方向了
    * 3.5 中间异常点纠正，根据前后正常的内容进行推算异常点方法见 @link{error2right}
    * 3.6 多线路纠正，车辆存在替车等情况，或者上传多线路，利用弗雷歇定理进行识别纠正 方法见@link{routeConfirm}
    * 转换成公交到站数据
    *
    * @return df
    */
  def toStation(bStation: Broadcast[Array[StationData]]): DataFrame = {
    val time2date = udf { (upTime: String) =>
      upTime.split("T")(0)
    }

    //未来使用备选线路id库，给df内部操作的时候使用广播进去，不然会出错
    val carIdAndRoute = busDataCleanUtils.data.select(col("carId"), time2date(col("upTime")).as("upTime"), col("route")).distinct().rdd
      .groupBy(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime"))).collectAsMap()
    val bCarIdAndRoute = busDataCleanUtils.data.sparkSession.sparkContext.broadcast(carIdAndRoute)

    //对每辆车的时间进行排序，进行shuffleSort还是进行局部sort呢？
    val groupByKey = busDataCleanUtils.data.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime")).split("T")(0))

    groupByKey.flatMapGroups((s, it) => {

      val maybeLineId = bCarIdAndRoute.value.get(s).get

      //局部sort，对每一辆车的每天的数据进行排序，内存应该占不大
      var gps = it.toArray[Row].sortBy(row => row.getString(row.fieldIndex("upTime"))).map(_.mkString(","))
      val stationMap = bStation.value.groupBy(sd => sd.route + "," + sd.direct)

      maybeLineId.foreach { route =>
        val maybeRouteUp = stationMap.getOrElse(route.getString(route.fieldIndex("route")) + ",up", Array())
        val maybeRouteDown = stationMap.getOrElse(route.getString(route.fieldIndex("route")) + ",down", Array())
        //选取数据的前两个不同位置的点
        val firstSplit = gps.head.split(",")
        val firstLon = firstSplit(8).toDouble
        val firstLat = firstSplit(9).toDouble
        var secondLon = 0.0
        var secondLat = 0.0
        var count = 0
        var flag = true
        while (flag) {
          if (gps.length - 1 <= count) {
            flag = false
            secondLon = firstLon
            secondLat = firstLat
          }
          val secondSplit = gps(count).split(",")
          secondLon = secondSplit(8).toDouble
          secondLat = secondSplit(9).toDouble
          if (secondLon != firstLon || secondLat != firstLat)
            flag = false
          count += 1
        }
        //确认初始化方向,false->down,true->up
        var upOrDown = true
        if (!maybeRouteUp.isEmpty) {
          val Array(one, _*) = maybeRouteUp.filter(sd => sd.stationSeqId == 1)
          val oneDis = LocationUtil.distance(firstLon, firstLat, one.stationLon, one.stationLat)
          val twoDis = LocationUtil.distance(secondLon, secondLat, one.stationLon, one.stationLat)
          if (oneDis <= 200.0)
            upOrDown = true
          else if (oneDis > twoDis && !maybeRouteDown.isEmpty)
            upOrDown = false
        }
        if (!maybeRouteDown.isEmpty) {
          val Array(one, _*) = maybeRouteDown.filter(sd => sd.stationSeqId == 1)
          val oneDis = LocationUtil.distance(firstLon, firstLat, one.stationLon, one.stationLat)
          val twoDis = LocationUtil.distance(secondLon, secondLat, one.stationLon, one.stationLat)
          if (oneDis <= 200.0)
            upOrDown = false
          else if (oneDis > twoDis)
            upOrDown = true
        }
        //方向匹配
        var firstSD: StationData = null //前一记录的站点信息
      var firstDirect = "up" //初始化方向
      var firstIndex = 0 //前一记录站点顺序
      var isStatus = false //是否进入运营状态
      var endStationCount = 0 //到达总站后所必须保留的记录数
      var isArrival = false //是否到达终点站
      var stopCount = 0 //停止阈值

        gps = gps.map { row =>
          var result = row
          val split = row.split(",")
          val lon = split(8).toDouble
          val lat = split(9).toDouble
          val time = split(11)
          val min2 = Array(Double.MaxValue, Double.MaxValue)
          val min2SD = new Array[StationData](2)
          if (upOrDown) {
            if (!isStatus) {
              for (i <- 0 until maybeRouteUp.length - 1) {
                val ld = LocationUtil.distance(lon, lat, maybeRouteUp(i).stationLon, maybeRouteUp(i).stationLat)
                val rd = LocationUtil.distance(lon, lat, maybeRouteUp(i + 1).stationLon, maybeRouteUp(i + 1).stationLat)
                if (min2(0) > ld && min2(1) > rd) {
                  min2(0) = ld
                  min2(1) = rd
                  min2SD(0) = maybeRouteUp(i)
                  min2SD(1) = maybeRouteUp(i + 1)
                  firstIndex = min2SD(0).stationSeqId - 1
                  if (firstSD != null && min2SD(1).stationSeqId < firstSD.stationSeqId && math.abs(min2SD(1).stationSeqId - maybeRouteUp.length) < 3 && !maybeRouteDown.isEmpty) {
                    upOrDown = false
                    isStatus = false
                    firstIndex = 0
                  }
                }
                if (firstSD != null && min2SD(0).stationSeqId == 2 && firstSD.stationSeqId == 2) {
                  isStatus = true
                }
              }
            } else {
              for (i <- firstIndex to firstIndex + 1) {
                var indexedSeq = i
                if (indexedSeq == maybeRouteUp.length - 1) {
                  indexedSeq = maybeRouteUp.length - 2
                  endStationCount += 1
                }
                val ld = LocationUtil.distance(lon, lat, maybeRouteUp(indexedSeq).stationLon, maybeRouteUp(indexedSeq).stationLat)
                val rd = LocationUtil.distance(lon, lat, maybeRouteUp(indexedSeq + 1).stationLon, maybeRouteUp(indexedSeq + 1).stationLat)
                if (min2(0) > ld && min2(1) > rd) {
                  min2(0) = ld
                  min2(1) = rd
                  min2SD(0) = maybeRouteUp(indexedSeq)
                  min2SD(1) = maybeRouteUp(indexedSeq + 1)
                  firstIndex = min2SD(0).stationSeqId - 1
                  if (rd < 100.0 && math.abs(min2SD(1).stationSeqId - maybeRouteUp.length) < 1)
                    isArrival = true
                  if (firstSD != null && endStationCount > 2 && isArrival && !maybeRouteDown.isEmpty) {
                    upOrDown = false
                    isStatus = false
                    firstIndex = 0
                    endStationCount = 0
                    isArrival = false
                  }
                  if (firstSD != null && min2SD(1).stationSeqId == firstSD.stationSeqId && Math.abs(min2SD(1).stationSeqId - maybeRouteUp.length) < 2) {
                    stopCount += 1
                    if (stopCount > 15) {
                      isStatus = false
                      stopCount = 0
                    }
                  }
                }
              }
            }
          } else {
            if (!isStatus) {
              for (i <- 0 until maybeRouteDown.length - 1) {
                val ld = LocationUtil.distance(lon, lat, maybeRouteDown(i).stationLon, maybeRouteDown(i).stationLat)
                val rd = LocationUtil.distance(lon, lat, maybeRouteDown(i + 1).stationLon, maybeRouteDown(i + 1).stationLat)
                if (min2(0) > ld && min2(1) > rd) {
                  min2(0) = ld
                  min2(1) = rd
                  min2SD(0) = maybeRouteDown(i)
                  min2SD(1) = maybeRouteDown(i + 1)
                  firstIndex = min2SD(0).stationSeqId - 1
                  if (firstSD != null && min2SD(1).stationSeqId < firstSD.stationSeqId && math.abs(min2SD(1).stationSeqId - maybeRouteDown.length) < 3) {
                    upOrDown = true
                    isStatus = false
                    firstIndex = 0
                  }
                }
                if (firstSD != null && min2SD(0).stationSeqId == 2 && firstSD.stationSeqId == 2) {
                  isStatus = true
                }
              }
            } else {
              for (i <- firstIndex to firstIndex + 1) {
                var indexedSeq = i
                if (indexedSeq == maybeRouteDown.length - 1) {
                  indexedSeq = maybeRouteDown.length - 2
                  endStationCount += 1
                }
                val ld = LocationUtil.distance(lon, lat, maybeRouteDown(indexedSeq).stationLon, maybeRouteDown(indexedSeq).stationLat)
                val rd = LocationUtil.distance(lon, lat, maybeRouteDown(indexedSeq + 1).stationLon, maybeRouteDown(indexedSeq + 1).stationLat)
                if (min2(0) > ld && min2(1) > rd) {
                  min2(0) = ld
                  min2(1) = rd
                  min2SD(0) = maybeRouteDown(indexedSeq)
                  min2SD(1) = maybeRouteDown(indexedSeq + 1)
                  firstIndex = min2SD(0).stationSeqId - 1
                  if (rd < 100.0 && math.abs(min2SD(1).stationSeqId - maybeRouteDown.length) < 1)
                    isArrival = true
                  if (firstSD != null && endStationCount > 2 && isArrival) {
                    upOrDown = true
                    isStatus = false
                    firstIndex = 0
                    endStationCount = 0
                    isArrival = false
                  }
                  if (firstSD != null && min2SD(1).stationSeqId == firstSD.stationSeqId && Math.abs(min2SD(1).stationSeqId - maybeRouteDown.length) < 2) {
                    stopCount += 1
                    if (stopCount > 15) {
                      isStatus = false
                      stopCount = 0
                    }
                  }
                }
              }
            }
          }
          //异常方向点识别
          if (min2.max < Double.MaxValue) {
            var resultDirect = min2SD(0).direct
            if (firstSD != null && firstSD.stationSeqId > min2SD(1).stationSeqId && firstDirect.equals(resultDirect)) {
              if (resultDirect.equals("up"))
                resultDirect = resultDirect + "OrDown"
              else if (resultDirect.equals("down"))
                resultDirect = resultDirect + "OrUp"
              firstDirect = resultDirect
            } else if (firstSD != null && firstSD.stationSeqId >= min2SD(1).stationSeqId && firstDirect.contains("Or")) {
              resultDirect = firstDirect
            } else {
              firstDirect = min2SD(0).direct
            }
            result = result + "," + min2SD(0).route + "," + resultDirect + "," + min2SD(0).stationSeqId + "," + min2(0) + "," + min2SD(1).stationSeqId + "," + min2(1)
          }
          firstSD = min2SD(1)
          result
        }
      }

      //中间异常点纠正
      val err2right = error2right(gps)

      //多线路筛选与分趟
      val finalResult = routeConfirm(err2right, stationMap, maybeLine = maybeLineId.size)

      finalResult.iterator
      //gps.iterator
    })
      //.count()
      .rdd.saveAsTextFile("D:/testData/公交处/toBusArrival")

    busDataCleanUtils.data
  }

  /**
    * 把分趟后的数据转成公交到站数据
    * 原理：
    * ------A-------B-------C----- gps点
    * --a-----b-------c------d---- 公交站点
    * 首先取gpsA点与B，计算AB与各站点的距离ld与rd，AB的距离pd
    * diff = ld+rd-pd，取最小的diff，作为站点的到站gps点
    * 因为diff最小说明两gps点刚好在站点的左右两边。
    * 所以AB两gps点就是线路站点b的到站点，BC作为c的到站点。
    *
    * @param toStation 分趟后的df
    * @return
    */
  def toBusArrivalData(toStation: Array[String], stationMap: Map[String, Array[StationData]]): Array[String] = {
    toStation.groupBy { s =>
      val split = s.split(",")
      split(split.length - 1)
    }.flatMap { s =>
      val list = s._2.sortBy(s => s.split(",")(11))
      val map = new mutable.HashMap[String, Int]()
      list.foreach { s =>
        val key = s.split(",")(17)
        map.put(key, map.getOrElse(key, 0) + 1)
      }
      val up = map.getOrElse("up", 0)
      val down = map.getOrElse("down", 0)
      var direct = "up"
      if (up < down)
        direct = "down"
      val result = new ArrayBuffer[String]()
      for (i <- 0 until s._2.length - 1) {
        val first = list(i).split(",")
        val next = list(i + 1).split(",")
        val firstLon = first(8).toDouble
        val firstLat = first(9).toDouble
        val nextLon = next(8).toDouble
        val nextLat = next(9).toDouble
        val stationArray = stationMap.getOrElse(first(16) + "," + direct, Array())
        var minDiff = Double.MaxValue
        var standLocation = 0
        stationArray.foreach { sd =>
          val ld = LocationUtil.distance(firstLon, firstLat, sd.stationLon, sd.stationLat)
          val rd = LocationUtil.distance(nextLon, nextLat, sd.stationLon, sd.stationLat)
          val pd = LocationUtil.distance(firstLon, firstLat, nextLon, nextLat)
          val diff = ld + rd - pd
          if (diff < minDiff) {
            minDiff = diff
            standLocation = sd.stationSeqId
          }
        }
        result += list(i) + "," + standLocation + "," + minDiff
      }
      result += list(s._2.length - 1) + ",0,0"
      result
    }.toArray
  }

  /**
    * 道路车速
    */
  def speed(): Unit = {

  }

  /**
    * 投币乘客O数据
    */
  def coinsPassengerO(): Unit = {

  }

  /**
    * 刷卡乘客O数据
    */
  def cardPassengerO(): Unit = {

  }

  /**
    * 道路车流
    */
  def trafficFlow(): Unit = {

  }

  /**
    * 公交乘客O数据
    */
  def busOData(): Unit = {
    coinsPassengerO()
    cardPassengerO()
  }

  /**
    * 乘客住址工作地
    */
  def passengerLocation(): Unit = {

  }

  /**
    * 公交车OD数据
    */
  def toOD(): Unit = {

  }

  /**
    * 分趟算法结果评价验证方法
    * 说明：只做趟次划分正确性的验证，对趟次进行可视化
    * 效率比较慢，最好在zeppelin上面用
    * 必须传入 toStation后的数据
    *
    * @param carId 车牌号
    * @param date 日期
    * @param toStation 分趟后数据
    * @param bMapStation 站点信息
    * @return df（TripVisualization）
    */
  def tripConfirm(carId: String, date: String, toStation: DataFrame, bMapStation: Broadcast[Map[String, Array[StationData]]]): DataFrame = {
    val dateContains = udf { (upTime: String) =>
      upTime.contains(date)
    }
    toStation.filter(col("carId") === carId && dateContains(col("upTime"))).select("carId", "lon", "lat", "route", "direct", "upTime", "tripId")
      .sort("upTime").groupByKey(row => row.getString(row.fieldIndex("carId")))
      .flatMapGroups { (key, it) =>
        val arr = new ArrayBuffer[Point]()
        val result = new ArrayBuffer[TripVisualization]()
        val row_0 = it.take(1).toArray
        val mapKey = row_0(0).getString(row_0(0).fieldIndex("route")) + "," + row_0(0).getString(row_0(0).fieldIndex("direct"))
        val station = bMapStation.value.getOrElse(mapKey, Array()).map(sd => Point(sd.stationLon, sd.stationLat))
        var index = 0
        it.foreach { data =>
          val point = new Point(data.getDouble(data.fieldIndex("lon")), data.getDouble(data.fieldIndex("lat")))
          arr.+=(point)
          val dis = FrechetUtils.compareGesture(arr.toArray, station)
          result += TripVisualization(index, data.getInt(data.fieldIndex("tripId")), dis)
          index += 1
        }
        result.iterator
      }.toDF()
  }

  /**
    * gps的三角形原理定位位置，过滤无法定位点
    *
    * @param gps        gps
    * @param maybeRoute 线路信息表
    * @return
    */
  def selectPointInfo(gps: Array[String], maybeRoute: Array[StationData]): Array[String] = {
    gps.filter { row =>
      var result = false
      val split = row.split(",")
      val lon = split(8).toDouble
      val lat = split(9).toDouble
      var index = -1
      for (i <- 0 until maybeRoute.length - 1) {
        val ld = LocationUtil.distance(lon, lat, maybeRoute(i).stationLon, maybeRoute(i).stationLat)
        val rd = LocationUtil.distance(lon, lat, maybeRoute(i + 1).stationLon, maybeRoute(i + 1).stationLat)
        val pd = LocationUtil.distance(maybeRoute(i).stationLon, maybeRoute(i).stationLat, maybeRoute(i + 1).stationLon, maybeRoute(i + 1).stationLat)
        if (ld + rd < 1.2 * pd) {
          index = i
        }
      }
      if (index >= 0) {
        result = true
      }
      result
    }
  }

  /**
    * 去掉经纬度重复数据，不考虑时间
    * 主要用于对历史道路识别匹配加速计算
    *
    * @return df("carId","lon","lat","route","upTime 格式：yyyy-MM-dd")
    */
  def distinctLonLat(): DataFrame = {
    val upTime2Data = udf { (upTime: String) => upTime.split("T")(0) }
    busDataCleanUtils.toDF.select(col("carId"), col("lon"), col("lat"), col("route"), upTime2Data(col("upTime")).as("upTime")).distinct()
  }
}

object RoadInformation {
  def apply(busDataCleanUtils: BusDataCleanUtils): RoadInformation = new RoadInformation(busDataCleanUtils)
}