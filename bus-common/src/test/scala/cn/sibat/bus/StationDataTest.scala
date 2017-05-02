package cn.sibat.bus

import org.apache.spark.sql.SparkSession

/**
  * 公交到站测试类
  * Created by kong on 2017/5/2.
  */
object StationDataTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("StationDataTest").master("local[*]").getOrCreate()
    spark.read.textFile()
  }
}
