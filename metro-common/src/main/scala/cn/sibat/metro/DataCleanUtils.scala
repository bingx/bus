package cn.sibat.metro

import org.apache.spark.sql.DataFrame

/**
  * 深圳通数据清洗工具
  * Created by wing1995 on 2017/5/4.
  */
class DataCleanUtils(val data: DataFrame) {

  /**
    * 将清洗完的数据返回
    * @return
    */
  def toDF(): DataFrame = this.data

  /**
    * 构造伴生对象，返回对象本身，实现链式写法
    * @param df
    * @return
    */
  private def newUtils(df: DataFrame): DataCleanUtils = new DataCleanUtils(df)

  /**
    * 针对地铁数据
    * 根据站点ID补全上车站点名称为“None”的记录，
    * 根据路线ID修正乘车路线名称错误的记录
    * @return
    */
  def recoveryData(): DataCleanUtils = {
    val missingData = this.data.filter("siteName == \"None\"")
    newUtils(missingData)
  }
}

object BusDataCleanUtils {
  def apply(data: DataFrame): DataCleanUtils = new DataCleanUtils(data)
}