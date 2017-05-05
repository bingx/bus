package cn.sibat.bus

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * 从mysql中读取站点静态数据
  * Created by kong on 2017/5/3.
  */
object DirectNULLToPredict {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/path/to/my").appName("DirectNULLToPredict").master("local[*]").getOrCreate()
    val url = ""
    val prop = new Properties()
    val lineDF = spark.read.jdbc(url, "line", prop)
    val lineStopDF = spark.read.jdbc(url, "line_stop", prop)
    val stationDF = spark.read.jdbc(url, "station", prop)
    lineDF.createOrReplaceTempView("line")
    lineStopDF.createOrReplaceTempView("line_stop")
    stationDF.createOrReplaceTempView("station")
    val sql = "select l.ref_id,l.direction,s.station_id,ss.name,s.stop_order,ss.lat,ss.lon from line l,line_stop s,station ss where l.id=s.line_id AND s.station_id=ss.id order by l.ref_id,l.direction,s.stop_order"
    spark.sql(sql).show()
  }
}
