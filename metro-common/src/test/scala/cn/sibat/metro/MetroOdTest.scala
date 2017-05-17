package cn.sibat.metro

import org.apache.spark.sql.SparkSession

/**
  * Created by wing1995 on 2017/5/10.
  */
object MetroOdTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
      .appName("Spark SQL Test")
      .master("local[*]")
      .getOrCreate()
    val ds = spark.read.textFile("E:\\trafficDataAnalysis\\cleanData\\part-00000").cache()
    val metroOD = new MetroOD
    val dfOD = metroOD.calMetroOD(ds)
    val sums = dfOD.count()
    println(sums)
    //val dfTimeDiff = metroOD.getTimeDiff(dfOD)
    //dfTimeDiff.describe("timeDiff").show()
  }
}
//2231405 sum
//2213739 merged
