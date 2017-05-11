package cn.sibat.metro

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

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
      (268017, "123456", "2017-01-01 03:21:41", "21", "36222", "地铁三号线", "None"),
      (268018, "456789", "2017-01-01 14:50:41", "22", "36233", "地铁二号线", "车公庙"),
      (268018, "234567", "2017-01-02 03:21:41", "21", "36222", "地铁二号线", "车公庙"),
      (268019, "123456", "2017-01-02 04:21:41", "22", "36233", "地铁二号线", "None"))
      .toDF("siteId", "recordCode", "cardTime", "transType", "cardCode", "routeName", "siteName")

    val station = Seq(
      (268017, "地铁二号线", "深圳北"),
      (268018, "地铁二号线", "车公庙"),
      (268019, "地铁二号线", "西丽")
    ).toDF("siteId", "routeNameStatic", "siteNameStatic")

    //恢复“siteName”和“routeName”字段记录
    var result = SZT.join(station, Seq("siteId")) //join not add union(insert records)
      .withColumn("routeName", when(col("routeName") =!= col("routeNameStatic"), col("routeNameStatic")).otherwise(col("routeName")))
      .withColumn("siteName", when(col("siteName") === "None", col("siteNameStatic")).otherwise(col("siteName")))
      .select("siteId", "recordCode", "cardTime", "cardCode", "routeName", "siteName")

    //生成新的日期列
    val time2date = udf { (time: String) => time.split(" ")(0) }
    result = result.withColumn("dateStamp", unix_timestamp($"cardTime", "yyyy-MM-dd HH:mm:ss"))
    result = result.withColumn("oldDate", time2date(col("cardTime"))) //旧日期
    result = result.withColumn("beginTime", unix_timestamp($"oldDate", "yyyy-MM-dd") + 60 * 60 * 4) //开始时间
    result = result.withColumn("endTime", unix_timestamp($"oldDate", "yyyy-MM-dd") + 60 * 60 * 28)
    //结束时间
    val pureData = result.withColumn("date", when($"dateStamp" > $"beginTime" && $"endTime" > $"dateStamp", $"oldDate")
      .otherwise(date_format(($"dateStamp" - 60 * 60 * 24).cast("timestamp"), "yyyy-MM-dd")))
      .drop("dateStamp", "oldData", "beginTime", "endTime")

    case class Record(siteId: String, recordCode: String, cardTime: String, cardCode: String, routeName: String,
                      siteName: String, date: String)

    val dataRDD = pureData.rdd.map(x => Record(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString, x(5).toString, x(6).toString))
    val pairs = dataRDD.map(records => (records.cardCode, records))
    val ODs = pairs.groupByKey.mapValues(records => {
      val sortedArr = records //对每一个组RDD[Iterator]转换Array引用类型，然后将数组按照打卡时间排序
        .toArray
        .sortBy(_.cardTime)

      //将数组里面的每一条单独的记录连接成字符串
      val stringArr = sortedArr.map(record => record.siteId + ',' + record.recordCode + ',' + record.cardTime + ',' + record.cardCode + ',' + record.routeName + ',' + record.siteName + ',' + record.date)
      def generateOD(arr: Array[String]): Array[String] = {
        val newRecords = new ArrayBuffer[String]()
        for (i <- 0 until arr.length by 2 if(arr.length % 2 == 0)) {
          val emptyString = new StringBuilder()
          val OD = emptyString.append(stringArr(i)).append(',').append(stringArr(i + 1)).toString()
          newRecords += OD
        }
        newRecords.toArray
      }
      generateOD(stringArr)
    }
    )
    ODs.values.map(x => x.mkString("\n")).foreach(println)
  }
}