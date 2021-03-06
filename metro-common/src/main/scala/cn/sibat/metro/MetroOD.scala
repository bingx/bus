package cn.sibat.metro

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, _}

import scala.collection.mutable.ArrayBuffer

/**
  * 地铁乘客OD计算
  * Created by wing1995 on 2017/5/10.
  */
class MetroOD extends Serializable {

  /**
    * 乘客OD信息的生成
    * 根据刷卡卡号进行分组，将每位乘客的OD信息转换为数组，按刷卡时间排序，
    * 将排序好的数组转换为逗号分隔的字符串，最后将字符串两两合并生成乘客OD信息
    * @param ds 清洗后地铁乘客的DataSet
    * @return
    */
  def calMetroOD(ds: Dataset[String]): DataFrame = {
    import ds.sparkSession.implicits._
    //生成乘客OD记录
    val dataRDD = ds.rdd.map(x => x.split(",")).map(x => MetroSZT(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7)))
    val pairs = dataRDD.map(records => (records.cardCode, records))
    val ODs = pairs.groupByKey.flatMap(records => {
      val sortedArr = records._2 //对每一个组RDD[Iterator]转换Array引用类型，然后将数组按照打卡时间排序
        .toArray
        .sortBy(_.cardTime)

      //将数组里面的每一条单独的记录连接成字符串
      val stringRecord = sortedArr.map(record => record.recordCode + ',' + record.cardCode + ',' + record.terminalCode + ',' + record.transType + ','
        + record.cardTime + ',' + record.routeName + ',' + record.siteName + ',' + record.GateMark)
      /**
        * 将乘客的相邻两个刷卡记录两两合并为一条字符串格式的OD记录
        *
        * @param arr 每一个乘客当天的刷卡记录组成的数组
        * @return 每一个乘客当天使用深圳通乘坐地铁产生的OD信息组成的数组
        */
      def generateOD(arr: Array[String]): Array[String] = {
        val newRecords = new ArrayBuffer[String]()
        for (i <- 1 until arr.length) {
          val emptyString = new StringBuilder()
          val OD = emptyString.append(arr(i-1)).append(',').append(arr(i)).toString()
          newRecords += OD
        }
        newRecords.toArray
      }
      generateOD(stringRecord)
    }
    )
    val row = ODs.map(line => line.split(",")).map(line => OD(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9), line(10), line(11), line(12), line(13), line(14), line(15)))
    row.filter(line => line.transType == "21" && line.outTransType == "22").toDF()
  }

  /**
    * 生成进站与出站的时间差列，保留出时间差大于3小时以及出入站点不同的记录
    * @param df 由乘客刷卡生成的OD数据构成的DataFrame
    * @return
    */
  def getTimeDiff(df: DataFrame): DataFrame = {
    val timeUtils = new TimeUtils
    val timeDiffUDF = udf((startTime: String, endTime: String) => timeUtils.calTimeDiff(startTime, endTime))
    val ODsCalTimeDiff = df.withColumn("timeDiff", timeDiffUDF(col("cardTime"), col("outCardTime")))
    val timeLessThan3 = ODsCalTimeDiff.filter(col("timeDiff") < 3)
    val inNotEqualToOut = timeLessThan3.filter(col("siteName") =!= col("outSiteName"))
    inNotEqualToOut
  }
}

case class OD(recordCode: String, cardCode: String, terminalCode: String, transType: String,
              cardTime: String, routeName: String, siteName: String, GateMark: String,
              outRecordCode: String, outCardCode: String, outTerminalCode: String, outTransType: String,
              outCardTime: String, outRouteName: String, outSiteName: String, outGateMark: String
             )

