package cn.sibat.bus

/**
  * 站点数据
  * 线路，来回站点标识（上行01,下行02），站点Id，站点名称，站点序号，站点经度，站点纬度
  * Created by kong on 2017/4/11.
  *
  * @param route        线路
  * @param direct       方向
  * @param stationId    站点ID
  * @param stationName  站点名称
  * @param stationSeqId 站点序号
  * @param stationLon   站点经度
  * @param stationLat   站点纬度
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
  * @param tripId         班次号
  */
case class BusArrivalData(raw: String, carId: String, arrivalTime: String, leaveTime: String, nextStation: String
                          , firstStation: String, arrivalStation: String, stationSeqId: Long, tripId: String)

case class Trip(carId: String, route: String, direct: String, firstSeqIndex: Int, ld: Double, nextSeqIndex: Int, rd: Double, tripId: Int) {
  override def toString: String = route + "," + direct + "," + firstSeqIndex + "," + ld + "," + nextSeqIndex + "," + rd
}

/**
  * 趟次可视化实体类
  *
  * @param index 序号
  * @param tripId 趟次
  * @param frechetDistance 弗雷歇距离
  */
case class TripVisualization(index:Int,tripId:Int,frechetDistance:Double)

/**
  * 公交到站可视化实体
  * @param carId 车牌号
  * @param lon 经度
  * @param lat 纬度
  * @param route 线路
  * @param direct 方向
  * @param upTime 上传时间
  * @param tripId 班次号
  */
case class BusArrivalForVisual(carId:String, lon:Double, lat:Double, route:String, direct:String, upTime:String, tripId:Int)

/**
  * Created by kong on 2017/6/22.
  */
case class CaseClassSet()
