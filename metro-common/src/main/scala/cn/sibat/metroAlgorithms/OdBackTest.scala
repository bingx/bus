package cn.sibat.metroAlgorithms

import org.apache.spark.sql.SparkSession
import huzhen.code.MainThread

/**
  * OD反推算法在Spark中的应用
  * Created by wing1995 on 2017/6/12.
  */
object OdBackTest extends App {
  //为了计算运行程序所用时间
  val start = System.currentTimeMillis

  val spark = SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
    .appName("Spark SQL Test")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val ds = spark.read.textFile("metro-common/src/main/resources/testData")
  val data = ds.map(value => value.split(","))

  var count = 0 //计数
  var amount = 0
  val result = data.rdd.map(line => {
    amount += 1
    var resultPath = ""
    val cardNo = line(0)
    val matchedLine = MainThread.CheckAllPathNew(line)
    if (matchedLine != null) {
      count += 1
      resultPath = MainThread.functionPrintData(cardNo, matchedLine)
    } else System.out.println("match failure!")
    resultPath
  })
  result.foreach(println)
  println("成功估计路径的OD数：" + count)
  println("OD总数：" + amount)
  //result.saveAsTextFile("test_20170102")
  System.out.println("time:" + (System.currentTimeMillis - start) / 1000 / 60.0 + "min") //数据：57，程序运行时间：0.16666666666666666min
}