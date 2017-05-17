package cn.sibat.metro

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wing1995 on 2017/5/10.
  */
object MetroOdTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OD_APP").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("E:\\trafficDataAnalysis\\cleanData\\part-00000").cache()
    val OD = new MetroOD(lines).calMetroOD
    println(OD.count())
    //OD.take(100).foreach(println)
    sc.stop()
  }
}
//2231405 sum
//2213740 merged
