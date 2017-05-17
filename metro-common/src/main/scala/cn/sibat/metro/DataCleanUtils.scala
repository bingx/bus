package cn.sibat.metro

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, _}

/**
  * 深圳通数据清洗工具，使用链式写法，给每一个异常条件（清洗规则）添加方法
  * Created by wing1995 on 2017/5/4.
  */
class DataCleanUtils(val data: DataFrame) {

  /**
    * 将清洗完的数据返回
    * @return DataFrame
    */
  def toDF: DataFrame = this.data

  /**
    * 构造伴生对象，返回对象本身，实现链式写法
    * @param df 清洗后的DataFrame
    * @return 原对象 DataCleanUtils
    */
  private def newUtils(df: DataFrame): DataCleanUtils = new DataCleanUtils(df)

  /**
    * 针对深圳通原始数据，添加日期列
    * 将深圳通数据文件中的打卡时间为每天的4：00到次日4：00的记录记为当日日期，4:00之前的日期指定为前一天的日期
    * 将此日期添加到DataFrame，并将其他列删除
    * @return
    */
  def addDate(): DataCleanUtils = {
    val time2date = udf{(time: String) => time.split(" ")(0)}
    val addStamp = this.data.withColumn("dateStamp", unix_timestamp(col("cardTime"), "yyyy-MM-dd HH:mm:ss"))
    val addDate = addStamp.withColumn("oldDate", time2date(col("cardTime"))) //旧日期
    val addBeginTime = addDate.withColumn("beginTime", unix_timestamp(col("oldDate"), "yyyy-MM-dd") + 60 * 60 * 4)
    val addEndTime = addBeginTime.withColumn("endTime", unix_timestamp(col("oldDate"), "yyyy-MM-dd") + 60 * 60 * 28)
    val addNewDate = addEndTime.withColumn("date", when(col("dateStamp") > col("beginTime") && col("endTime") > col("dateStamp"), col("oldDate"))
      .otherwise(date_format((col("dateStamp") - 60 * 60 * 24).cast("timestamp"), "yyyy-MM-dd")))
      .drop("dateStamp", "oldDate", "beginTime", "endTime")
    newUtils(addNewDate)
  }

  /**
    * 针对地铁数据
    * 根据站点ID唯一对应站点名称，利用站点ID补全站点名称为“None”的字段，统一所有站点名称
    * 根据站点ID唯一确定路线名称，利用站点ID修正路线名称错误的字段
    * @param dataStation 静态地铁站点数据
    * @return 原对象DataCleanUtils
    */
  def recoveryData(dataStation: DataFrame): DataCleanUtils = {
    val siteIdCol = udf { (terminalCode: String) => terminalCode.slice(0, 6) }
    val tmpData = this.data.withColumn("siteId", siteIdCol(col("terminalCode")))
    var recoveryData = tmpData.join(dataStation, Seq("siteId"))
    recoveryData = recoveryData.withColumn("siteName", when(col("siteName").equalTo("siteNameStatic"), col("siteName")).otherwise(col("siteNameStatic")))
      .withColumn("routeName", when(col("routeName").equalTo(col("routeNameStatic")), col("routeName")).otherwise(col("routeNameStatic")))
      .select("recordCode", "cardCode", "terminalCode", "transType", "cardTime", "routeName", "siteName", "GateMark", "date")
    newUtils(recoveryData)
  }
}

object DataCleanUtils {
  def apply(data: DataFrame): DataCleanUtils = new DataCleanUtils(data)
}