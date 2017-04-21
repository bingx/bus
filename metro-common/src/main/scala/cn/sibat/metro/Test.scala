package cn.sibat.metro
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//统计字符出现次数
object Test {

  def count(inputFile: String) {
    val conf = new SparkConf().setAppName("example").setMaster("spark://172.20.36.248:4041").set("spark.executor.memory","1g")
    val sc = new SparkContext(conf)
    val line = sc.textFile(inputFile)

    val counts = line.flatMap(_.split(',')).map((_, 1)).reduceByKey(_ + _)
    counts.collect.foreach(println)
    sc.stop()
  }

  def main(args: Array[String]): Unit ={
    val inputFile = "linkage"
    count(inputFile)
  }
}
