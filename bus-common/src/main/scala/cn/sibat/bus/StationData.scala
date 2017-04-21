package cn.sibat.bus

import org.apache.spark.sql.DataFrame
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
  * Created by kong on 2017/4/11.
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
    * 转换成公交到站数据
    */
  def toStation(stationDF: DataFrame): DataFrame = {

    //计算线路中两个点（p1、p2）与gps点（p3）最近的点
    // 组成p1p3->ld、p1p2->pd、p2p3->rd =>ld+rd<1.2*pd
    val isRightRoute = udf { (route: String, lon: Double, lat: Double) => {
      var flag = false
      val min2 = Array(0.0, 0.0)
      var lon_lat = new ArrayBuffer[String]()
      lon_lat = lon_lat ++ Seq("null", "null")
      stationDF.select(route).foreach(s => {
        val s_lon = s.getDouble(s.fieldIndex("lon"))
        val s_lat = s.getDouble(s.fieldIndex("lat"))
        val dis = LocationUtil.distance(lon, lat, s_lon, s_lat)
        if (min2.min > dis) {
          min2(min2.indexOf(min2.min)) = dis
          lon_lat = lon_lat.tail.+=(s_lon + "," + s_lat)
          flag = true
        }
      })
      flag
    }
    }
    busDataCleanUtils.data.withColumn("isRight", isRightRoute(col("route"), col("lon"), col("lat")))
    stationDF.select(col("route") === "route")
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