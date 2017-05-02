package cn.sibat.bus

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * 站点数据
  * 线路，站点Id，站点名称，站点序号，站点经度，站点纬度
  *
  * @param route        线路
  * @param stationId    站点ID
  * @param stationName  站点名称
  * @param stationSeqId 站点序号
  * @param stationLon   站点经度
  * @param stationLat   站点纬度
  *
  *                     Created by kong on 2017/4/11.
  */
case class StationData(route: String, stationId: String, stationName: String, stationSeqId: Long, stationLon: Double, stationLat: Double)

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
  def joinInfo(): Unit = {
    //shp文件，进行道路匹配
    busDataCleanUtils.data.sparkSession.read.textFile("")

  }

  /**
    * 这里是要自己传进来还是自己去加载好呢？
    *
    * 转换成公交到站数据
    */
  def toStation(bStation: Broadcast[Array[StationData]]): DataFrame = {

    import busDataCleanUtils.data.sparkSession.implicits._
    //对每辆车的时间进行排序，进行shuffleSort还是进行局部sort呢？
    busDataCleanUtils.data.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime")).split("T")(0))
      .mapGroups((s, it) => {
        val result = new ArrayBuffer[String]()
        var firstRow: Row = null
        //局部sort，对每一辆车的每天的数据进行排序，内存应该占不大
        it.toBuffer[Row].sortBy(row => row.getString(row.fieldIndex("upTime"))).foreach(row => {
          val stationInfo = bStation.value
          val stationInfoMap = stationInfo.groupBy(sd => sd.route)

          /**
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
            */
          val min2 = Array(Double.MaxValue, Double.MaxValue)
          var array = new ArrayBuffer[StationData]()

          val lon = row.getDouble(row.fieldIndex("lon"))
          val lat = row.getDouble(row.fieldIndex("lat"))
          stationInfo.foreach(sd => {
            val rd = LocationUtil.distance(sd.stationLon, sd.stationLat, lon, lat)

            /** =============================线路确认 ============================= */
            if (rd < min2.max) {
              min2(min2.indexOf(min2.max)) = rd
              if (array.size < 2)
                array.+=(sd)
              else
                array = array.tail.+=(sd)
            }

          })

          val pd = LocationUtil.distance(array(0).stationLon, array(0).stationLat, array(1).stationLon, array(1).stationLat)

          //线路线路标记
          var realRoute = ""
          if (min2.sum < 1.2 * pd) {
            realRoute = array(0).route + ";" + array(1).route
          }

          var firstSD = stationInfoMap.get(realRoute).get(0)
          var minLocation = Double.MaxValue
          var lastIndex = ""
          var curIndex = ""
          var index = 0

          if (result.isEmpty) {
            firstRow = row
          }
          var minLast = Double.MaxValue
          var minCur = Double.MaxValue
          var lastLinkIndex = firstSD.stationSeqId
          var curLinkIndex = firstSD.stationSeqId

          stationInfoMap.get(realRoute).get.foreach(sd => {
            val rd = LocationUtil.distance(sd.stationLon, sd.stationLat, lon, lat)

            /** ==============================位置确认============================== */
            if (index != 0) {
              val ld = LocationUtil.distance(firstSD.stationLon, firstSD.stationLat, lon, lat)
              val sdDis = LocationUtil.distance(firstSD.stationLon, firstSD.stationLat, sd.stationLon, sd.stationLat)
              val diff = rd + ld - sdDis
              if (diff < minLocation) {
                lastIndex = sd.stationSeqId + "," + ld
                curIndex = sd.stationSeqId + "," + rd
                minLocation = diff
              }
              firstSD = sd
            }

            /** =============================方向确认============================= */
            val lastDis = LocationUtil.distance(firstRow.getDouble(firstRow.fieldIndex("lon")), firstRow.getDouble(firstRow.fieldIndex("lat")), sd.stationLon, sd.stationLat)
            val curDis = rd
            if (lastDis < minLast) {
              minLast = lastDis
              lastLinkIndex = sd.stationSeqId
            }
            if (curDis < minCur) {
              minCur = curDis
              curLinkIndex = sd.stationSeqId
            }
            firstRow = row
            index = 1
          })

          var direct = "unknown"
          if (lastLinkIndex < curLinkIndex || (lastLinkIndex == curLinkIndex && minLast > minCur)) {
            direct = "1"
            //val direct = "last->cur"
          } else if (lastLinkIndex > curLinkIndex || (lastLinkIndex == curLinkIndex && minLast > minCur)) {
            direct = "2"
            //val direct = "cur->last"
          }
          result.+=(row.mkString(",") + "," + realRoute + "," + lastIndex + "," + curIndex + "," + direct)
        })

        result.toArray
      }).flatMap(it => {
      //数据格式row,realRoute,lastIndex,lastDis,curIndex,curDis,direct
      val result = new ArrayBuffer[BusArrivalData]()
      it.filter { str =>
        val split = str.split(",")
        val curDis = split(split.length - 2)
        curDis.toDouble < 50.0
      }
      it
    })

    busDataCleanUtils.data
  }

  /**
    * 公交班次
    */
  def busService(): Unit = {

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
}

object RoadInformation {
  def apply(busDataCleanUtils: BusDataCleanUtils): RoadInformation = new RoadInformation(busDataCleanUtils)
}