package cn.sibat.metro
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Created by wing1995 on 2017/4/20.
  */
object Word {
  def countWords(sc: SparkContext) = {
    // Load our input data
    val input = sc.textFile("E:/trafficDataAnalysis/SZTDataCheck/testData.txt")
    // Split it up into words
    val words = input.flatMap(line => line.split(","))
    // Transform into pairs and count
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("result")
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    countWords(sc)
  }

}
