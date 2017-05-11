package cn.sibat.metro

import org.apache.spark.sql.SparkSession

/**
  * Created by wing1995 on 2017/5/10.
  */
object Metro_OD_Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
      .appName("Spark SQL Test")
      .master("local[*]")
      .getOrCreate()
    //读取原始数据，格式化
    val ds = spark.read.textFile("E:\\trafficDataAnalysis\\testData\\")
    val ds_station = spark.read.textFile("E:\\trafficDataAnalysis\\subway_station")
    val df_metro_SZT = DataFormatUtils(ds).trans_metro_SZT
    val df__metro_station = DataFormatUtils(ds_station).trans_metro_station
    val pureMetroData = DataCleanUtils(df_metro_SZT).addDate().recoveryData(df__metro_station).toDF
    pureMetroData.orderBy("cardCode", "cardTime").filter("date = \"2017-01-01\"").show(100)
  }
}
