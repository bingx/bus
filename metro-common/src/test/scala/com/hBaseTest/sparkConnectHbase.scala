package com.hBaseTest

import cn.sibat.metroUtils.TimeUtils

import org.apache.spark.{SparkConf, SparkContext}
import it.nerdammer.spark.hbase._


import java.lang._

/**
  * 连接spark连接hbase
  * 实现从本地文本文件读取数据存储到hbase
  * Created by wing1995 on 2017/6/22.
  */
class sparkConnectHbase() extends Serializable{

  /**
    * 根据原始数据文件名称的命名时间筛选符合要求的数据
    * 筛选条件是以文件名称的命名时间确定为当天时间，GPS的定位时间若在三天以前或今天以后则过滤
    * @param today 文件称的命名时间
    * @param gpsTimestamp gps定位的时间戳
    * @return Boolean
    */
  def checkValidTime(today: String, gpsTimestamp: Int): Boolean = {
    val daySeconds = 86400 // 60 * 60 * 24
    val now = TimeUtils.apply.time2stamp(today, "yyyyMMdd").toInt

    if( now - 3 * daySeconds > gpsTimestamp || now + daySeconds < gpsTimestamp)
      return false
    true
  }

  /**
    * 验证GPS的有效性，过滤经纬度不再中国境内的数据
    * @param gpsLat 纬度
    * @param gpsLng 经度
    * @return Boolean
    */
  def checkValidGPS(gpsLat: Float, gpsLng: Float): Boolean = {
    val minLng = 73.48
    val maxLng = 135.09
    val minLat = 17.73
    val maxLat = 53.54

    if(gpsLat < minLat || gpsLat > maxLat || gpsLng < minLng || gpsLng > maxLng)
      return false
    true
  }

  /**
    * 根据不同GPS的定位日期存储到不同日期命名的表
    * @param tableName 表名
    * @param gpsTime gps定位时间
    * @return Boolean
    */
  def splitTable(tableName: String, gpsTime: String): Boolean = {
    val gpsTimeDate = TimeUtils.apply.stamp2time(TimeUtils.apply.time2stamp(gpsTime, "yyyy-MM-dd HH:mm:ss"), "yyyyMMdd")
    if (tableName != gpsTimeDate) {
      return false
    }
    true
  }
}

object sparkConnectHbase {
  def apply(): sparkConnectHbase = new sparkConnectHbase()
}

object sparkConnectHBaseTest {
  def main(args: Array[String]): Unit = {
    // Initializing HBASE Configuration variables
    val HBASE_DB_HOST = args(1) // 172.20.36.248:9090
    val HBASE_TABLE_TODAY= args(0) //20170509
    val HBASE_TABLE_BACKONE = TimeUtils.apply.stamp2time(TimeUtils.apply.time2stamp(HBASE_TABLE_TODAY, "yyyyMMdd") - 86400, "yyyyMMdd")
    val HBASE_TABLE_BACKTWO = TimeUtils.apply.stamp2time(TimeUtils.apply.time2stamp(HBASE_TABLE_TODAY, "yyyyMMdd") - 2 * 86400, "yyyyMMdd")
    val HBASE_TABLE_BACKTHREE = TimeUtils.apply.stamp2time(TimeUtils.apply.time2stamp(HBASE_TABLE_TODAY, "yyyyMMdd") - 3 * 86400, "yyyyMMdd")
    val HBASE_COLUMN_FAMILY = "original"

    val APP_NAME = "hbaseLoadingData"
    // setting spark application
    val sparkConf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]")
    //initialize the spark context
    val sparkContext = new SparkContext(sparkConf)
    //Configuring Hbase host with Spark/Hadoop Configuration
    sparkContext.hadoopConfiguration.set("spark.hbase.host", HBASE_DB_HOST)

    val filePath = "metro-common/src/test/resources/STRING"
    val fileName = Array(filePath, HBASE_TABLE_TODAY).mkString("_")
    val rdd = sparkContext.textFile(fileName)
      .map(line => {
      val bytes = line.getBytes("GBK")
      val decodedLine = new String(bytes, "UTF-8")
      decodedLine
    }).map(line => line.split(","))

    val checkedRDD = rdd.filter(arr => {
      var isCheckedFlag = false
      try {
        val gpsTimeStr = Array("20" , arr(11)).mkString("")
        val gpsTimestamp = TimeUtils.apply.time2stamp(gpsTimeStr, "yyyy-MM-dd HH:mm:ss")
        val gps_lng = arr(8).toFloat
        val gps_lat = arr(9).toFloat
        isCheckedFlag = sparkConnectHbase.apply().checkValidTime(HBASE_TABLE_TODAY, gpsTimestamp.toInt) || sparkConnectHbase.apply().checkValidGPS(gps_lat, gps_lng)
      } catch {
        case e: ArrayIndexOutOfBoundsException => println("This row contains missing value!")
        case _ => println("Unexpected error happened!")
      }
      isCheckedFlag
    })

    var missingRecordAmount = 0
    val tupleRDD = checkedRDD.map(arr => {
      var row_key = ""
      var gps_rec_timestamp = 0
      var gps_lng = 0.0f
      var gps_lat = 0.0f
      var gps_speed = 0.0f
      var gps_angle = 0.0f
      var gps_readable_time = ""
      var bus_line_id = ""
      var bus_speed = 0.0f

      try {
        val k_type = "01" // bus
        val k_city = "01" // ShenZhen
        val bus_id = arr(3)
        val gps_time_str = Array("20" , arr(11)).mkString("")
        val gps_timestamp = TimeUtils.apply.time2stamp(gps_time_str, "yyyy-MM-dd HH:mm:ss").toInt

        row_key = Array(k_type, k_city, bus_id, gps_timestamp).mkString("|")
        gps_rec_timestamp = TimeUtils.apply.time2stamp(arr(0), "yyyy-MM-dd HH:mm:ss").toInt
        gps_lng = arr(8).toFloat
        gps_lat = arr(9).toFloat
        gps_speed = arr(12).toFloat
        gps_angle = arr(13).toFloat
        gps_readable_time = gps_time_str
        bus_line_id = arr(5)
        bus_speed = arr(14).toFloat
      } catch {
        case e1: IndexOutOfBoundsException => missingRecordAmount += 1
        case e2: NullPointerException => println("value is null")
        case _ => println("Unexpected error happened!")
      }
      (row_key, gps_rec_timestamp, gps_lng, gps_lat, gps_speed, gps_angle, gps_readable_time, bus_line_id, bus_speed)
    })

    tupleRDD.filter(arr => sparkConnectHbase.apply().splitTable(HBASE_TABLE_TODAY, arr._7))
      .toHBaseTable(HBASE_TABLE_TODAY)
      .toColumns("GPS_RECV_TIMESTAMP", "GPS_LNG", "GPS_LAT", "GPS_SPEED", "GPS_ANGLE", "GPS_TIME", "BUS_LINEID", "BUS_SPEED")
      .inColumnFamily(HBASE_COLUMN_FAMILY)
      .save()

    tupleRDD.filter(arr => sparkConnectHbase.apply().splitTable(HBASE_TABLE_BACKONE, arr._7))
      .toHBaseTable(HBASE_TABLE_BACKONE)
      .toColumns("GPS_RECV_TIMESTAMP", "GPS_LNG", "GPS_LAT", "GPS_SPEED", "GPS_ANGLE", "GPS_TIME", "BUS_LINEID", "BUS_SPEED")
      .inColumnFamily(HBASE_COLUMN_FAMILY)
      .save()

    tupleRDD.filter(arr => sparkConnectHbase.apply().splitTable(HBASE_TABLE_BACKTWO, arr._7))
      .toHBaseTable(HBASE_TABLE_BACKTWO)
      .toColumns("GPS_RECV_TIMESTAMP", "GPS_LNG", "GPS_LAT", "GPS_SPEED", "GPS_ANGLE", "GPS_TIME", "BUS_LINEID", "BUS_SPEED")
      .inColumnFamily(HBASE_COLUMN_FAMILY)
      .save()

    tupleRDD.filter(arr => sparkConnectHbase.apply().splitTable(HBASE_TABLE_BACKTHREE, arr._7))
      .toHBaseTable(HBASE_TABLE_BACKTHREE)
      .toColumns("GPS_RECV_TIMESTAMP", "GPS_LNG", "GPS_LAT", "GPS_SPEED", "GPS_ANGLE", "GPS_TIME", "BUS_LINEID", "BUS_SPEED")
      .inColumnFamily(HBASE_COLUMN_FAMILY)
      .save()
    println(missingRecordAmount)
  }
}
