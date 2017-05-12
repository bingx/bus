package cn.sibat.bus

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 站点数据
  * 线路，来回站点标识（上行01,下行02），站点Id，站点名称，站点序号，站点经度，站点纬度
  *
  * @param route        线路
  * @param direct       方向
  * @param stationId    站点ID
  * @param stationName  站点名称
  * @param stationSeqId 站点序号
  * @param stationLon   站点经度
  * @param stationLat   站点纬度
  *
  *                     Created by kong on 2017/4/11.
  */
case class StationData(route: String, direct: String, stationId: String, stationName: String, stationSeqId: Int, stationLon: Double, stationLat: Double)

/**
  * 公交刷卡数据
  *
  * @param rId         记录编码
  * @param lId         卡片逻辑编码
  * @param term        终端编码
  * @param tradeType   交易类型
  * @param time        拍卡时间
  * @param companyName 公司名称
  * @param route       线路名称
  * @param carId       车牌号
  */
case class BusCardData(rId: String, lId: String, term: String, tradeType: String, time: String, companyName: String, route: String, carId: String)

/**
  * 公交到站数据
  *
  * @param raw            列名
  * @param carId          车牌号
  * @param arrivalTime    到达时间
  * @param leaveTime      离开时间
  * @param nextStation    下一站点
  * @param firstStation   前一站点
  * @param arrivalStation 到达站点
  * @param stationSeqId   站点序号
  * @param buses          班次号
  */
case class BusArrivalData(raw: String, carId: String, arrivalTime: String, leaveTime: String, nextStation: String
                          , firstStation: String, arrivalStation: String, stationSeqId: Long, buses: String)

class RoadInformation(busDataCleanUtils: BusDataCleanUtils) {

  import busDataCleanUtils.data.sparkSession.implicits._

  def joinInfo(): Unit = {
    //shp文件，进行道路匹配
    busDataCleanUtils.data.sparkSession.read.textFile("")

  }

  /**
    * 历史线路确认
    * 计算线路中两个点（p1、p2）与gps点（p3）最近的点
    * 组成p1p3->ld、p1p2->pd、p2p3->rd =>ld+rd<1.2*pd
    * 符合条则认为这个点是在这条线路上
    */
  def routeConfirm(bStation: Broadcast[Array[StationData]]): Unit = {
    distinctLonLat().groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime")))
      .flatMapGroups((s, it) => {
        //局部sort，对每一辆车的每天的数据进行排序，内存应该占不大
        val gps = it.toBuffer[Row].sortBy(row => row.getString(row.fieldIndex("upTime"))).map(_.mkString(","))
        val stationMap = bStation.value.groupBy(sd => sd.route + "," + sd.direct)
        val map = new mutable.HashMap[String, Array[Int]]() //命中线路，站点集合
        val upRoute = new mutable.HashSet[String]() //上传站点集合

        stationMap.foreach { route =>
          gps.foreach { row =>
            val split = row.split(",")
            val lon = split(1).toDouble
            val lat = split(2).toDouble
            upRoute.+=(split(3))
            val min2 = Array(Double.MaxValue, Double.MaxValue)
            var array = new ArrayBuffer[StationData]()
            route._2.foreach { sd =>
              val dis = LocationUtil.distance(lon, lat, sd.stationLon, sd.stationLat)
              if (dis < min2.max) {
                min2(min2.indexOf(min2.max)) = dis
                if (array.size < 2)
                  array.+=(sd)
                else
                  array = array.tail.+=(sd)
              }
              if (dis < 50.0 && (sd.stationSeqId == 1 || sd.stationSeqId == route._2.length)) {
                var get = map.getOrElse(route._1, Array[Int]())
                get = get ++ Seq(0, sd.stationSeqId)
                map.update(route._1, get)
              }
            }
            if (array.size > 1) {
              val pd = LocationUtil.distance(array(0).stationLon, array(0).stationLat, array(1).stationLon, array(1).stationLat)
              if (min2.sum < 1.2 * pd) {
                var get = map.getOrElse(route._1, Array[Int]())
                get = get ++ Seq(array(0).stationSeqId, array(1).stationSeqId)
                map.update(route._1, get)
              }
            }
          }
        }
        val rate = new ArrayBuffer[String]()
        //站点命中率=gps命中站点数/线路总站点数
        map.foreach { m =>
          val lineSize = stationMap.getOrElse(m._1, Array()).length
          if (m._2.contains(0))
            rate += s + "," + m._2.mkString(";") + "," + m._1 + ",0;" + m._2.toSet.size + "," + lineSize + "," + (m._2.toSet.size.toDouble - 1) / lineSize + "," + upRoute.mkString(",")
          else
            rate += s + "," + m._2.mkString(";") + "," + m._1 + "," + m._2.toSet.size + "," + lineSize + "," + m._2.toSet.size.toDouble / lineSize + "," + upRoute.mkString(",")
        }
        rate.sortBy(s => s.split(",")(s.split(",").length - upRoute.size - 1)).iterator
      }).rdd.repartition(1).saveAsTextFile("D:/testData/公交处/rate/BCK127+")
  }

  /**
    *
    * 1.线路确认
    * 计算线路中两个点（p1、p2）与gps点（p3）最近的点
    * 组成p1p3->ld、p1p2->pd、p2p3->rd =>ld+rd<1.2*pd
    * 符合条则认为这个点是在这条线路上
    * 2.方向确认
    * 取两个gps点（lastPoint，curPoint）分别与线路最近的一个点（可能同一个点）
    * 线路上点的index为lastIndex,curIndex,距离lastDis,curDis
    * lastIndex < curIndex or lastIndex = curIndex && lastDis < curDis
    * 则方向是lastIndex->curIndex,否则lastIndex <- curIndex
    * 3.位置确认
    * 计算线路中两个点（p1、p2）与gps点（p3）最近的点
    * 距离组成p1p3->ld、p1p2->pd、p2p3->rd => diff = ld+rd-pd
    * min(diff)就是车的位置
    *
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
    busDataCleanUtils.data.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime")).split("T")(0))
      .flatMapGroups((s, it) => {

        val maybeLineId = bCarIdAndRoute.value.get(s).get

        //局部sort，对每一辆车的每天的数据进行排序，内存应该占不大
        var gps = it.toBuffer[Row].sortBy(row => row.getString(row.fieldIndex("upTime"))).map(_.mkString(","))
        val stationMap = bStation.value.groupBy(sd => sd.route + "," + sd.direct)

        maybeLineId.foreach { route =>
          var maybeRoute = stationMap.getOrElse(route.getString(route.fieldIndex("route")) + ",up", Array())
          if (maybeRoute.isEmpty) {
            maybeRoute = stationMap.getOrElse(route.getString(route.fieldIndex("route")) + ",down", Array())
          }
          gps = gps.map { row =>
            var result = row
            val split = row.split(",")
            val lon = split(8).toDouble
            val lat = split(9).toDouble
            val min2 = Array(Double.MaxValue, Double.MaxValue)
            val min2SD = new Array[StationData](2)
            for (i <- 0 until maybeRoute.length - 1) {
              val ld = LocationUtil.distance(lon,lat,maybeRoute(i).stationLon,maybeRoute(i).stationLat)
              val rd = LocationUtil.distance(lon,lat,maybeRoute(i+1).stationLon,maybeRoute(i+1).stationLat)
              if (min2(0) > ld && min2(1) > rd) {
                min2(0) = ld
                min2(1) = rd
                min2SD(0) = maybeRoute(i)
                min2SD(1) = maybeRoute(i+1)
              }
            }
            if (min2.max < Double.MaxValue) {
              result = result + "," + min2SD(0).route+","+min2SD(0).direct + "," + min2SD(0).stationSeqId + "," + min2(0) + "," + min2SD(1).stationSeqId + "," + min2(1)
            }
            result
          }
        }
        gps.iterator
      })
      //          .foreach(row => {
      //          val stationInfo = bStation.value
      //          val stationInfoMap = stationInfo.groupBy(sd => sd.route + "," + sd.direct)
      //          maybeLineId += row.getString(row.fieldIndex("route"))+","+"up"
      //          maybeLineId += row.getString(row.fieldIndex("route"))+","+"down"
      //

      //
      //          //线路线路标记
      //          var realRoute = ""
      //          val lon = row.getDouble(row.fieldIndex("lon"))
      //          val lat = row.getDouble(row.fieldIndex("lat"))
      //          stationInfoMap.foreach { line =>
      //            val min2 = Array(Double.MaxValue, Double.MaxValue)
      //            var array = new ArrayBuffer[StationData]()
      //            line._2.foreach { sd =>
      //              val rd = LocationUtil.distance(sd.stationLon, sd.stationLat, lon, lat)
      //
      //              /** =============================线路确认 ============================= */
      //              if (rd < min2.max) {
      //                min2(min2.indexOf(min2.max)) = rd
      //                if (array.size < 2)
      //                  array.+=(sd)
      //                else
      //                  array = array.tail.+=(sd)
      //              }
      //            }
      //            if (array.size > 1) {
      //              val pd = LocationUtil.distance(array(0).stationLon, array(0).stationLat, array(1).stationLon, array(1).stationLat)
      //              if (min2.sum < 1.2 * pd) {
      //                maybeLineId += line._1
      //              }
      //            }
      //          }
      //
      //          try {
      //            maybeLineId.foreach { lineId =>
      //              var firstSD = stationInfoMap.get(realRoute).get(0)
      //              var minLocation = Double.MaxValue
      //              var lastIndex = ""
      //              var curIndex = ""
      //              var index = 0
      //
      //              if (result.isEmpty) {
      //                firstRow = row
      //              }
      //              var minLast = Double.MaxValue
      //              var minCur = Double.MaxValue
      //              var lastLinkIndex = firstSD.stationSeqId
      //              var curLinkIndex = firstSD.stationSeqId
      //
      //              stationInfoMap.get(realRoute).get.foreach(sd => {
      //                val rd = LocationUtil.distance(sd.stationLon, sd.stationLat, lon, lat)
      //
      //                /** ==============================位置确认============================== */
      //                if (index != 0) {
      //                  val ld = LocationUtil.distance(firstSD.stationLon, firstSD.stationLat, lon, lat)
      //                  val sdDis = LocationUtil.distance(firstSD.stationLon, firstSD.stationLat, sd.stationLon, sd.stationLat)
      //                  val diff = rd + ld - sdDis
      //                  if (diff < minLocation) {
      //                    lastIndex = sd.stationSeqId + "," + ld
      //                    curIndex = sd.stationSeqId + "," + rd
      //                    minLocation = diff
      //                  }
      //                  firstSD = sd
      //                }
      //
      //                /** =============================方向确认============================= */
      //                val lastDis = LocationUtil.distance(firstRow.getDouble(firstRow.fieldIndex("lon")), firstRow.getDouble(firstRow.fieldIndex("lat")), sd.stationLon, sd.stationLat)
      //                val curDis = rd
      //                if (lastDis < minLast) {
      //                  minLast = lastDis
      //                  lastLinkIndex = sd.stationSeqId
      //                }
      //                if (curDis < minCur) {
      //                  minCur = curDis
      //                  curLinkIndex = sd.stationSeqId
      //                }
      //                firstRow = row
      //                index = 1
      //              })
      //
      //              var direct = "unknown"
      //              if (lastLinkIndex < curLinkIndex || (lastLinkIndex == curLinkIndex && minLast > minCur)) {
      //                direct = "1"
      //                //val direct = "last->cur"
      //              } else if (lastLinkIndex > curLinkIndex || (lastLinkIndex == curLinkIndex && minLast > minCur)) {
      //                direct = "2"
      //                //val direct = "cur->last"
      //              }
      //              result.+=(row.mkString(",") + "," + realRoute + "," + lastIndex + "," + curIndex + "," + direct)
      //            }
      //          } catch {
      //            case _: Throwable => result.+=(row.mkString(",") + "," + realRoute)
      //          }
      //        })
      //
      //        result.toArray
      //      }).flatMap(it => {
      //      //数据格式row,realRoute,lastIndex,lastDis,curIndex,curDis,direct
      //      //      val result = new ArrayBuffer[BusArrivalData]()
      //      //      it.filter { str =>
      //      //        val split = str.split(",")
      //      //        val curDis = split(split.length - 2)
      //      //        curDis.toDouble < 50.0
      //      //      }
      //      it
      //    })
      //.count()
      .rdd.saveAsTextFile("D:/testData/公交处/toStation2")

    busDataCleanUtils.data
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