package cn.sibat.bus

import org.apache.spark.sql.SparkSession

/**
  * 测试BusDataCleanUtil的方法
  * 都通过
  * Created by kong on 2017/5/2.
  */
object BusCleanTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/path/to/my").appName("BusCleanTest").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val data = spark.read.textFile("D:/testData/公交处/data/2016-12-01/*/*")
    val busDataCleanUtils = new BusDataCleanUtils(data.toDF())
    val format = busDataCleanUtils.dataFormat()
//    println(format.data.count())
//    println(format.zeroPoint().data.count())
//    format.zeroPoint().data.show()
    //println(format.errorPoint().data.count())
    //format.errorPoint().data.show()
//    println(format.filterStatus().data.count())
//    format.filterStatus().data.show()
    format.intervalAndMovement().data.show()
  }
}
