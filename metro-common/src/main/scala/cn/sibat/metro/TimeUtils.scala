package cn.sibat.metro

import java.text.SimpleDateFormat

import org.joda.time.DateTime


/**
  * 时间戳是指从格林威治时间1970年1月1日00时00分00秒至当前时刻的总秒数（精确到毫秒）
  * 相当于北京时间1970年1月1日08时00分00秒
  * Created by wing1995 on 2017/5/8.
  */
class TimeUtils extends Serializable{

  /**
    * 格式化字符串转换为时间戳
    * @param time 格式化字符串
    * @param timeFormat 时间戳
    * @return timeStamp
    */
  def time2stamp(time: String, timeFormat: String): Long = {
    val sdf = new SimpleDateFormat(timeFormat)
    val timeStamp = sdf.parse(time).getTime
    timeStamp
  }

  /**
    * 时间戳转换为格式化的字符串
    * @param timeStamp 时间戳
    * @param timeFormat 格式化字符串
    * @return timeString
    */
  def stamp2time(timeStamp: Long, timeFormat: String): String = {
    val timeString = new DateTime(timeStamp).toString(timeFormat)
    timeString
  }

  /**
    * 计算两个时间字符串之间的时间差
    * @param formerDate 早点的时间（字符串格式）
    * @param olderDate 晚点的时间（字符串格式）
    * @return timeDiff
    */
  def calTimeDiff(formerDate: String, olderDate: String): Float = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timeDiff = (sdf.parse(olderDate).getTime - sdf.parse(formerDate).getTime) / (3600f * 1000f) //得到小时为单位
    timeDiff
  }
}