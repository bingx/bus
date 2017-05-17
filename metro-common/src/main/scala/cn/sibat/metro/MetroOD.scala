package cn.sibat.metro

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

/**
  * 传过来的数据都是清洗后的地铁乘客刷卡记录
  * Created by wing1995 on 2017/5/10.
  */
class MetroOD(data: RDD[String]) {

  /**
    * 地铁乘客OD计算
    * 然后根据刷卡卡号进行分组，将每位乘客的OD信息转换为数组，按刷卡时间排序，
    * 将排序好的数组转换为逗号分隔的字符串，最后将字符串两两合并生成乘客OD信息
    * @return 所有乘客的OD记录
    */
  def calMetroOD: RDD[String] = {
    //生成乘客OD记录
    val dataRDD = data.map(x => x.split(",")).map(x => MetroSZT(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7)))
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
    ODs.map(line => line.split(",")).filter(line => line(3) == "21" && line(11) == "22").map(arr => arr.mkString(","))
  }
}

object MetroOD{
  def apply(data: RDD[String]): MetroOD = new MetroOD(data)
}
