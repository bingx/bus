package cn.sibat.metro

import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * 深圳通地铁刷卡数据的清洗工具
  * Created by wing1995 on 2017/4/26.
  */
class MetroDataCleanUtils(val data: DataFrame) {
  /**
    * 对深圳通数据进行格式化
    * 列名："recordCode", "logicCode", "terminalCode", "compId", "siteId", "transType", "cardTime", "compName", "siteName", "vehicleNum"
   */
  def dataFormat(data: Dataset[String])={

  }
}
