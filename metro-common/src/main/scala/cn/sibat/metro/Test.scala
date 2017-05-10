package cn.sibat.metro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, _}

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
    val ds = spark.read.textFile("E:\\trafficDataAnalysis\\testData\\")
    val df_SZT = DataFormatUtils(ds).trans_SZT
    DataCleanUtils(df_SZT).addDate.toDF.select("cardTime", "newDate")
//    val ds_station = spark.read.textFile("E:\\trafficDataAnalysis\\subway_station")
//    val df_metro = DataFormatUtils.apply.trans_metro_SZT(ds)
//    val df_station = DataFormatUtils.apply.trans_metro_station(ds_station)
//
//    执行数据恢复
//    val df_result = new DataCleanUtils(df).recoveryData(df_station).toDF

//    //测试SZT打卡时间分布
//    val colHour = udf {(cardTime: String) => cardTime.slice(11, 13)}
//    val df_SZT = DataFormatUtils.apply.trans_SZT(ds)
//    df_metro.withColumn("hour", colHour(col("cardTime"))).filter(col("hour") === "04").select("cardCode", "hour")
//      .join(df_metro, Seq("cardCode"), "inner")
//      .select("cardCode", "hour", "recordCode", "terminalCode", "transType", "cardTime", "routeName", "siteName", "GateMark")
//      .show()

  }
}

//4519343 result_subway
//698447 to_recovery
//698447 missing
//4519343 subway
//7358615 all