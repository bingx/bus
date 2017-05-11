package cn.sibat.metro

import org.joda.time.DateTime
import java.text.SimpleDateFormat

/**
  * 时间戳是指从格林威治时间1970年1月1日00时00分00秒至当前时刻的总秒数（精确到毫秒）
  * 相当于北京时间1970年1月1日08时00分00秒
  * Created by wing1995 on 2017/5/8.
  */
class TimeChange{
  def time2stamp(time: String, timeFormat: String): Long = {
    val sdf = new SimpleDateFormat(timeFormat)
    sdf.parse(time).getTime
  }

  def stamp2time(timeStamp: Long, timeFormat: String): String = {
    new DateTime(timeStamp).toString(timeFormat)
  }

  def calculateTime(formerDate: String, olderData: String): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val formerDate = "2017-01-01 16:13:52"
    val Date = "2017-01-01 16:14:52"
    val newDate = (sdf.parse(formerDate).getTime - sdf.parse(Date).getTime) / 1000  //转换成秒
    println(newDate)
  }
}

object TimeChange {
  def apply: TimeChange = new TimeChange()

  def main(args: Array[String]): Unit = {
    val formerDate = "2017-01-01" //只是指明日期的情况下，时间戳默认到00：00：00为止
    val olderDate = "2017-01-01 00:00:00"
    println(TimeChange.apply.stamp2time(1483200000000l, "yyyy-MM-dd"))
//    val stampFormer = TimeChange.apply.time2stamp(formerDate, "yyyy-MM-dd")
//    val stampOlder = TimeChange.apply.time2stamp(olderDate, "yyyy-MM-dd HH:mm:ss")
//    val stamp2time = TimeChange.apply.stamp2time(stampOlder, "yyyy-MM-dd")
//    println(stampFormer)
//    println(stampOlder)
//    println(stamp2time)
  }
}
