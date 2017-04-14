package cn.sibat.bus

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
case class BusArrivalData(raw:String,carId:String,arrivalTime:String,leaveTime:String,nextStation:String
                          ,firstStation:String,arrivalStation:String,stationSeqId:Long,buses:String)

class RoadInformation(busDataCleanUtils: BusDataCleanUtils) {
  def joinInfo(): Unit = {
    //shp文件，进行道路匹配
    busDataCleanUtils.data.sparkSession.read.textFile("")

  }

  /**
    * 转换成公交到站数据
    */
  def toStation(): Unit = {
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