package cn.sibat.metro
import org.apache.spark.sql.SparkSession

/**
  * Created by wing1995 on 2017/4/20
  */

object Test {
  def main(args: Array[String]) = {
    val spark = SparkSession
        .builder()
        .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
        .appName("Spark SQL Test")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    val df = spark.sparkContext
      .textFile("E:\\trafficDataAnalysis\\SZTDataCheck\\testData.txt")
      .map(_.split(","))
      .map(line => SZT(line(0), line(1), line(2), line(2).substring(0, 3), line(2).substring(0, 6), line(4), line(8), line(12), line(13), line(14)))
      .toDF()
    df.show()
    df.printSchema()
  }
}
