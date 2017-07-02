package cn.sibat.taxi

import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/4/17.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    val seq = Seq("1882928493,1441100186P82,6P82,2016-08-30 00:00:20,2016-08-30 00:06:17,3,3,00Сʱ007,0,null,3,2016-08-30 00:00:00",
      "1882928762,14411003615766440940,U8G7,2016-08-30 00:00:20,2016-08-30 00:07:35,3,6,00Сʱ008,1,null,1,2016-08-30 00:00:00",
      "1882941345,14411005666F9,66F9,2016-08-30 00:00:20,2016-08-30 00:07:35,3,0,00Сʱ010,0,null,1,2016-08-30 00:00:00")
    println(11)
  }
}
