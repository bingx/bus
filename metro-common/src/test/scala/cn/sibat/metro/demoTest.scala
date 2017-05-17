package cn.sibat.metro

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

case class Record(siteId: String, recordCode: String, cardTime: String, transType: String, cardCode: Int, routeName: String, siteName: String)
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
      (268017, "123456", "2017-01-01 04:21:41", "21", "36222", "地铁三号线", "None"),
      (268018, "456789", "2017-01-01 14:50:41", "22", "36233", "地铁二号线", "车公庙"),
      (268018, "234567", "2017-01-02 03:51:41", "21", "36222", "地铁二号线", "车公庙"),
      (268020, "134567", "2017-01-02 03:55:41", "22", "36233", "地铁四号线", "红花岭"),
      (268021, "134563", "2017-01-01 10:55:41", "21", "36233", "地铁四号线", "茶光村"),
      (268019, "123456", "2017-01-02 03:21:41", "21", "36233", "地铁二号线", "None")
    )
      .toDF("siteId", "recordCode", "cardTime", "transType", "cardCode", "routeName", "siteName")

    val station = Seq(
      (268017, "地铁二号线", "深圳北"),
      (268018, "地铁二号线", "车公庙"),
      (268019, "地铁二号线", "西丽"),
      (268020, "地铁四号线", "红花岭"),
      (268021, "地铁一号线", "茶光村")
    ).toDF("siteId", "routeNameStatic", "siteNameStatic")

    //恢复“siteName”和“routeName”字段记录
    val result = SZT.join(station, Seq("siteId")) //join not add union(insert records)
      .withColumn("routeName", when(col("routeName") =!= col("routeNameStatic"), col("routeNameStatic")).otherwise(col("routeName")))
      .withColumn("siteName", when(col("siteName") === "None", col("siteNameStatic")).otherwise(col("siteName")))
      .select("siteId", "recordCode", "cardTime", "transType", "cardCode", "routeName", "siteName")

    //生成新的日期列
    val time2date = udf { (time: String) => time.split(" ")(0) }
    var resultFinal = result.withColumn("dateStamp", unix_timestamp($"cardTime", "yyyy-MM-dd HH:mm:ss"))
    resultFinal = resultFinal.withColumn("oldDate", time2date(col("cardTime"))) //旧日期
    resultFinal = resultFinal.withColumn("beginTime", unix_timestamp($"oldDate", "yyyy-MM-dd") + 60 * 60 * 4) //开始时间
    resultFinal = resultFinal.withColumn("endTime", unix_timestamp($"oldDate", "yyyy-MM-dd") + 60 * 60 * 28)

    val pureData = resultFinal.withColumn("date", when($"dateStamp" > $"beginTime" && $"endTime" > $"dateStamp", $"oldDate")
      .otherwise(date_format(($"dateStamp" - 60 * 60 * 24).cast("timestamp"), "yyyy-MM-dd")))
      .drop("dateStamp", "oldData", "beginTime", "endTime")

    //生成乘客OD记录
    val dataRDD = result.rdd.map(x => Record(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString.toInt, x(5).toString, x(6).toString))
    val ODs = dataRDD.groupBy(records => records.cardCode).flatMap(records => {
      val sortedArr = records._2 //对每一个组RDD[Iterator]转换Array引用类型，然后将数组按照打卡时间排序
        .toArray
        .sortBy(_.cardTime)

      //将数组里面的每一条单独的记录连接成字符串
      val stringArr = sortedArr.map(record => record.siteId + ',' + record.recordCode + ',' + record.cardTime + ',' + record.cardCode + ',' + record.transType +',' +record.routeName + ',' + record.siteName)
      def generateOD(arr: Array[String]): Array[String] = {
        val newRecords = new ArrayBuffer[String]()
        for (i <- 1 until arr.length) {
          val emptyString = new StringBuilder()
          val OD = emptyString.append(arr(i-1)).append(',').append(arr(i)).toString()
          newRecords += OD
        }
        newRecords.toArray
      }
      generateOD(stringArr)
    }
    )
    ODs.map(x => x.split(",")).filter(line => line(4) == "21" && line(11) == "22").map(x => x.mkString(",")).foreach(println)
  }
}