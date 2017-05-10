package cn.sibat.metro

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType

/**
  * Created by wing1995 on 2017/5/8.
  */
object demoTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
      .appName("Spark SQL Test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val SZT = Seq(
      (268017, "123456", "2017-01-01 04:21:41", "36233", "地铁三号线", "None"),
      (268018, "456789", "2017-01-01 14:50:41", "36233", "地铁二号线", "车公庙"),
      (268018, "234567", "2017-01-02 04:21:41", "36222", "地铁二号线", "车公庙"),
      (268019, "123456", "2017-01-02 03:21:41", "36222", "地铁二号线", "None"))
      .toDF("siteId", "recordCode", "cardTime", "cardCode", "routeName", "siteName")

    val station = Seq(
      (268017, "地铁二号线", "深圳北"),
      (268018, "地铁二号线", "车公庙"),
      (268019, "地铁二号线", "西丽")
    ).toDF("siteId", "routeNameStatic", "siteNameStatic")
    //恢复数据
    var result = SZT.join(station, Seq("siteId")) //join not add union(insert records)
      .withColumn("routeName", when(col("routeName") =!= col("routeNameStatic"), col("routeNameStatic")).otherwise(col("routeName")))
      .withColumn("siteName", when(col("siteName") === "None", col("siteNameStatic")).otherwise(col("siteName")))
      .select("siteId", "recordCode", "cardTime", "cardCode", "routeName", "siteName")
    //生成时间戳
    val ts = unix_timestamp($"cardTime", "yyyy-MM-dd HH:mm:ss").cast("timestamp")
    result = result
      .withColumn("dateStamp", ts)
      .withColumn("dateColumn", col("dateStamp").cast(DateType))

  }
}
