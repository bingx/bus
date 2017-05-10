package cn.sibat.metro

import org.joda.time.DateTime
import java.text.SimpleDateFormat

/**
  * 时间戳是指从格林威治时间1970年1月1日00时00分00秒至当前时刻的总秒数（精确到毫秒）
  * 相当于北京时间1970年1月1日08时00分00秒
  * Created by wing1995 on 2017/5/8.
  */
object TimeChange{
  def time2stamp(time: String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.parse(time).getTime
  }

  def stamp2time(timeStamp: Long): String = {
    new DateTime(timeStamp).toString("yyyy-MM-dd")
  }

  def calculateTime(formerDate: String, olderData: String): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val formerDate = "2017-01-01 16:13:52"
    val Date = "2017-01-01 16:14:52"
    val newDate = (sdf.parse(formerDate).getTime - sdf.parse(Date).getTime) / 1000  //转换成秒
    println(newDate)
  }

  def main(args: Array[String]): Unit = {
    val stamp = time2stamp("2017-01-01")
    val time = stamp2time(stamp)
    println("2017-01-01的时间戳格式为" + stamp)
    println("2017-01-01的时间字符串格式为" + time)
  }
}
