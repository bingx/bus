package cn.sibat.metro

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
  * 主要是
  * Created by wing1995 on 2017/4/26.
  */
class DataFrameUtils {

}

case class SZT(recordCode: String, logicCode: String, terminalCode: String, compId: String, siteId: String,
               transType: String, cardTime: String, compName: String, siteName: String, vehicleCode: String
              )

