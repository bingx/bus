package cn.sibat.metroAlgorithms

import org.apache.spark.sql.SparkSession
import huzhen.code.MainThread
import org.apache.spark.{SparkConf, SparkContext}

/**
  * OD反推算法在Spark中的应用
  * Created by wing1995 on 2017/6/12.
  */
object OdBackTest extends App {
  //为了计算运行程序所用时间
  val start = System.currentTimeMillis

//  val spark = SparkSession
//    .builder()
//    .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
//    .appName("Spark SQL Test")
//    .master("local[*]")
//    .getOrCreate()
//  spark.sparkContext.setLogLevel("ERROR")
//  import spark.implicits._
//
//  //val ds = spark.read.textFile("metro-common/src/main/resources/testData")
//  val ds = spark.read.textFile("E:\\trafficDataAnalysis\\OdData\\2017-01-02")
//  val data = ds.map(value => value.split(","))

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("OD TEST"))
  val data = sc.textFile("E:\\trafficDataAnalysis\\OdData\\2017-01-02").map(line => line.split(","))

  var count = 0 //计数
  var amount = 0
  val result = data.map(line => {
    amount += 1
    var resultPath = ""
    val cardNo = line(0)
    val matchedLine = MainThread.CheckAllPathNew(line)
    if (matchedLine != null) {
      count += 1
      resultPath = MainThread.functionPrintData(cardNo, matchedLine)
    } else System.out.println("匹配失败! 对应OD记录的卡号为：" + cardNo)
    resultPath
  })
  //result.take(100000)//foreach(println)
  result.repartition(1).saveAsTextFile("E:\\trafficDataAnalysis\\routeData\\2017-01-02")
  println("成功估计路径的OD数：" + count)
  println("OD总数：" + amount)
  println("time:" + (System.currentTimeMillis - start) / 1000 / 60.0 + "min")
}
//SparkSession
//成功估计路径的OD数：1852725
//OD总数：1848421
//time:0.7666666666666667min
//SparkContext
//成功估计路径的OD数：1858299
//OD总数：1853831
//time:0.85min