package cn.sibat.metro

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col

/**
  * 对清洗后的深圳通数据进行客流统计分析
  * Created by wing1995 on 2017/5/23.
  */
class FlowAnalysis(ds: Dataset[String]) extends Serializable{
  import ds.sparkSession.implicits._
  //加载清洗后的数据，将数据格式化为DataFrame，时间解析为小时
  val formatData: DataFrame = this.ds.map(value => value.split(",")).map(x => SZTAddDate(x(1), x(2), x(3), x(4).slice(11, 13), x(5), x(6), x(8))).toDF()

  /**
    * 每一条线路每一个时段的客流量统计
    * @return flowsByPerRoutePerHour
    */
  def flowByRouteAndHour(): DataFrame = {
    val flowsByRouteAndHour = formatData.groupBy(col("routeName"), col("cardHour"), col("cardDate")).count()
    flowsByRouteAndHour
  }

  /**
    * 每一个站点每一个时段的客流量统计
    * @return flowsBySiteAndHour
    */
  def flowBySiteAndHour(): DataFrame = {
    val flowsBySiteAndHour = formatData.groupBy(col("siteName"), col("cardHour"), col("cardDate")).count()
    flowsBySiteAndHour
  }

  /**
    * 每一个站点对应不同路线不同时段的客流量统计（比如转乘站）
    * @return flowsBySiteRouteAndHour
    */
  def flowBySiteRouteAndHour(): DataFrame = {
    val flowsBySiteRouteAndHour = formatData.groupBy(col("siteName"), col("routeName"), col("cardHour"), col("cardDate")).count()
    flowsBySiteRouteAndHour
  }
}

//去除记录编码，添加刷卡日期，并将刷卡时间转换为小时
case class SZTAddDate(
                       cardCode: String, terminalCode: String, transType: String, cardHour: String, routeName: String, siteName: String, cardDate: String
                     )

object FlowAnalysis {
  def apply(ds: Dataset[String]): FlowAnalysis = new FlowAnalysis(ds)
}