package cn.sibat.metro

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wing1995 on 2017/4/20.
  */
class TestAgain {
  def test(inputFile: String) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val line = sc.textFile(inputFile)

    val counts = line.flatMap(_.split(',')).map((_, 1)).reduceByKey(_ + _)
    counts.collect().foreach(println)
    sc.stop()
  }
}

class test {
  val obj = new TestAgain
  obj.test("linkage")
}
