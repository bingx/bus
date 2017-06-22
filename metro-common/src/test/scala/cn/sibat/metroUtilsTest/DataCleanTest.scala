package cn.sibat.metroUtilsTest

import cn.sibat.metroUtils.{DataCleanUtils, DataFormatUtils}
import org.apache.spark.sql.SparkSession

/**
  * 测试数据输出情况
  * Created by wing1995 on 2017/4/20
  */
object DataCleanTest {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
      .appName("Spark SQL Test")
      .master("local[*]")
      .getOrCreate()

    //读取原始数据，格式化
    val ds = spark.read.textFile("E:\\trafficDataAnalysis\\testData") //原始数据
    val ds_station = spark.read.textFile("E:\\trafficDataAnalysis\\subway_station") //站点静态表
    val df_metro = DataFormatUtils(ds).transMetroSZT
    val df_station = DataFormatUtils(ds_station).transMetroStation

    //执行时间重新划分和数据恢复得到最终的清洗数据
    val df_clean = new DataCleanUtils(df_metro).addDate().recoveryData(df_station).toDF.filter("date = \"2017-01-02\"")
    //println(df_clean.count())
    df_clean.rdd.map(x => x.mkString(",")).repartition(1).saveAsTextFile("E:\\trafficDataAnalysis\\cleanData\\2017-01-02")
//    //测试SZT打卡时间分布
//    val colHour = udf {(cardTime: String) => cardTime.slice(11, 13)}
//    val df_SZT = DataFormatUtils.apply.trans_SZT(ds)
//    df_metro.withColumn("hour", colHour(col("cardTime"))).filter(col("hour") === "04").select("cardCode", "hour")
//      .join(df_metro, Seq("cardCode"), "inner")
//      .select("cardCode", "hour", "recordCode", "terminalCode", "transType", "cardTime", "routeName", "siteName", "GateMark")
//      .show()

  }
}
//20170101数据计数（以供参考）
//4519343 result_subway clean_subway 4462810
//698447 to_recovery
//698447 missing
//4519343 subway
//7358615 all