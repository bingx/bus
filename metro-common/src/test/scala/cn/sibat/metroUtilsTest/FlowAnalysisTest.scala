package cn.sibat.metroUtilsTest

import cn.sibat.metroUtils.FlowAnalysis
import org.apache.spark.sql.SparkSession

/**
  * Created by wing1995 on 2017/5/23.
  */
object FlowAnalysisTest {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
      .appName("Spark SQL Test")
      .master("local[*]")
      .getOrCreate()

    //读取原始数据，格式化
    val ds = spark.read.textFile("E:\\trafficDataAnalysis\\cleanData\\part-00000")
    val flowsByRouteAndHour = FlowAnalysis(ds).flowByRouteAndHour()
    val flowsBySiteAndHour = FlowAnalysis(ds).flowBySiteAndHour()
    val flowsBySiteRouteAndHour = FlowAnalysis(ds).flowBySiteRouteAndHour()
    flowsByRouteAndHour.show(200)
    //flowsBySiteAndHour.show()
    //flowsBySiteRouteAndHour.show()
  }
}
