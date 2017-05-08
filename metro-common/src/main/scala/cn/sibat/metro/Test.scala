package cn.sibat.metro

import org.apache.spark.sql.SparkSession

/**
  * 测试数据输出情况
  * Created by wing1995 on 2017/4/20
  */
object Test {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
      .appName("Spark SQL Test")
      .master("local[*]")
      .getOrCreate()

    //读取原始数据，格式化
    val ds = spark.read.textFile("E:\\trafficDataAnalysis\\testData")
    val ds_station = spark.read.textFile("E:\\trafficDataAnalysis\\subway_station")
    val df = DataFormatUtils.apply.trans_metro_SZT(ds)
    val df_station = DataFormatUtils.apply.trans_metro_station(ds_station)

    //执行数据清洗
    val df_result = new DataCleanUtils(df).recoveryData(df_station).toDF
    println(df_result.filter(df_result("siteName").contains("车公庙")).count())
  }
}

//4519343 result_subway
//698447 to_recovery
//698447 missing
//4519343 subway
//7358615 all