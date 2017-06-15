package cn.sibat.metroUtilsTest

import cn.sibat.metroUtils.MetroOD
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
      .master("local[2]")
      .getOrCreate()
    val ds = spark.read.textFile("E:\\trafficDataAnalysis\\cleanData\\2017-01-02\\part-00000")
    val metroOD = new MetroOD
    val dfOD = metroOD.calMetroOD(ds)
    val dfTimeDiff = metroOD.getTimeDiff(dfOD)
    dfTimeDiff.rdd.map(line => line.mkString(",")).repartition(1).saveAsTextFile("E:\\trafficDataAnalysis\\OdData\\2017-01-02")
  }
}


