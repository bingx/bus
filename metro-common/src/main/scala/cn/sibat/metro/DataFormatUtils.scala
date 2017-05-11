package cn.sibat.metro

import org.apache.spark.sql._

/**
  * DataSet => Scheme
  * spark.read.text读进来的则是Dataset数据结构，每一行都是一个元素，类型为Row。没有命名列
  * 将默认数据类型转换为业务所需的数据视图，筛选需要的字段，以及数据类型格式化
  * Created by wing1995 on 2017/5/4.
  */
class DataFormatUtils(data: Dataset[String]) {
  import data.sparkSession.implicits._
  /**
    * 默认将SZT刷卡产生的原始记录中的每一列贴上字段标签
    * 字段名称：0.记录编码 1.卡片逻辑编码 2.刷卡设备终端编码 3.公司编码 4.交易类型 5.交易金额 6.卡内余额
    * 7.未知字段 8.拍卡时间 9.成功标识 10.未知时间1 11.未知时间2 12.公司名称 13.站点名称 14.车辆编号
    * @return
    */
  def trans_SZT: DataFrame = {
    val SZT_df= data.map(_.split(","))
      .map(line => SZT(line(0), line(1), line(2), line(3), line(4), line(5).toDouble, line(6).toDouble,
        line(7), line(8), line(9), line(10), line(11), line(12), line(13), line(14)))
      .toDF()
    SZT_df
  }

  /**
    * 从原始数据中删选出地铁刷卡数据，并给地铁打卡数据贴上字段标签
    * 字段名称：0.记录编码 1.卡片逻辑编码 2.终端编码 3.交易类型 4.拍卡时间 5.线路名称 6.站点名称 7.闸机标识
    * @return
    */
  def trans_metro_SZT: DataFrame = {
    val metro_SZT_df= data.map(_.split(","))
      .filter(line => line(2).matches("2[46].*"))  //compId以"24"或"26"开头的数据
      .map(line => metro_SZT(line(0), line(1), line(2), line(4), line(8), line(12), line(13), line(14)))
      .toDF()
    metro_SZT_df
  }

  /**
    * 从原始数据中筛选出公交刷卡数据，并给公交刷卡数据贴上对应的字段标签
    * 字段名称：0.记录编码 1.卡片逻辑编码 2.终端编码 3.交易类型 4.拍卡时间 5.公司名称 6.线路名称 7.车牌号
    * @return
    */
  def trans_bus_SZT: DataFrame = {
    val bus_SZT_df= data.map(_.split(","))
      .filter(line => line(2).matches("2[235].*")) //compId以“22”或“23”和“25”开头的数据
      .map(line => bus_SZT(line(0), line(1), line(2), line(4), line(8), line(12), line(13), line(14)))
      .toDF()
    bus_SZT_df
  }

  /**
    * 字段名称：0.站点Id 1.站点名称 2.路线Id 3.路线名称
    * @return
    */
  def trans_metro_station: DataFrame = {
    val metro_station_df= data.map(_.split(","))
      .map(line => metro_station(line(0), line(1), line(2), line(3)))
      .toDF()
    metro_station_df
  }
}

case class SZT(recordCode: String, cardCode: String, terminalCode: String, compCode: String, transType: String, tranAmount: Double,
               cardBalance: Double, unknownField: String, cardTime: String, successSign: String, unknownTime1: String,
               unknownTime2: String, compName: String, siteName: String, vehicleCode: String
              )

case class metro_SZT(recordCode: String, cardCode: String, terminalCode: String, transType: String,
                     cardTime: String, routeName: String, siteName: String, GateMark: String
                    )

case class bus_SZT(recordCode: String, cardCode: String, terminalCode: String, transType: String,
                   cardTime: String, compName: String, routeName: String, licenseNum: String
                  )

case class metro_station(siteId: String, siteNameStatic: String, routeId: String, routeNameStatic: String)

object DataFormatUtils{
  def apply(data: Dataset[String]): DataFormatUtils = new DataFormatUtils(data)
}
