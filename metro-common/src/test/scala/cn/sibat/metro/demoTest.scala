package cn.sibat.metro

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

/**
  * Created by wing1995 on 2017/5/8.
  */
object demoTest extends App {
  val spark = SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
    .appName("Spark SQL Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val SZT = Seq(
    (268017, "123456", "地铁三号线", "None"),
    (268018, "456789", "地铁二号线", "车公庙"),
    (268018, "234567", "地铁二号线", "车公庙"),
    (268019, "123456", "地铁二号线", "None")).toDF("siteId", "recordCode", "routeName", "siteName")

  val station = Seq(
    (268017, "地铁二号线", "深圳北"),
    (268018, "地铁二号线", "车公庙"),
    (268019, "地铁二号线", "西丽")
  ).toDF("siteId", "routeNameStatic", "siteNameStatic")

  val result = SZT.join(station, Seq("siteId")) //insert records
  result.withColumn("routeName", when(col("routeName") =!= col("routeNameStatic"), col("routeNameStatic")).otherwise(col("routeName")))
    .withColumn("siteName", when(col("siteName") === "None", col("siteNameStatic")).otherwise(col("siteName")))
    .select("siteId", "recordCode", "routeName", "siteName").show()
}
