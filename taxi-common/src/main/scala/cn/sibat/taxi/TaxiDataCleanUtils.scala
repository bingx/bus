package cn.sibat.taxi

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
/** 出租车数据清洗工具
  * 异常条件在这里添加，使用链式写法
  * 应用不同场景，进行条件组合
  * Created by Lhh on 2017/5/3.
  */
class TaxiDataCleanUtils(val data:DataFrame) extends Serializable{
  import this.data.sparkSession.implicits._


  object TaxiDataCleanUtils {
    def apply(data:DataFrame): TaxiDataCleanUtils = new TaxiDataCleanUtils(data)
  }

  /**
    * 构造对象
    * 也可以利用伴生对象apply方法BusDataCleanUtils(df)
    * @param df
    * @return
    */
  private def newUtils(df: DataFrame): TaxiDataCleanUtils = new TaxiDataCleanUtils(df)

  case class TaxiData(carId:String,lon:Double,lat:Double,upTime:String,SBH:String,speed:Double,direction:Double,
                      locationStatus:Double,X:Double,SMICarid:Double,carStatus:Double,carColor:String)

  /**
    * 对公交车数据进行格式化，并转化成对应数据格式
    * 结果列名"carId","lon","lat","upTime","SBH","speed","direction","locationStatus",
    * "X","SMICarid","carStatus","carColor"
    *
    * @return TaxiDataCleanUtils
    *
    */
  def dataFormat():TaxiDataCleanUtils = {
    var colType = Array("String")
    colType = colType ++ ("Double," * 2).split(",") ++ ("String," * 2).split(",") ++ ("Double," * 6).split(",") ++ "String".split(",")
    val cols = Array("carId","lon","lat","upTime","SBH","speed","direction","locationStatus",
       "X","SMICarid","carStatus","carColor")
    newUtils(DataFrameUtils.apply.col2moreCol(data.toDF(),"value",colType,cols: _*))
  }

  /**
    * 过滤掉经纬度0.0，0.0的记录
    * @return
    */
  def zeroPoin(): TaxiDataCleanUtils = {
    newUtils(this.data.filter(col("lon") =!= 0.0 && col("lat") =!= 0.0))
  }

  /**
    * 过滤经纬度异常数据，异常条件为
    * 经纬度在中国范围内
    * 中国的经纬度范围纬度：3.86-53.55，经度：73.66-135.05
    * @return self
    */
  def errorsPoint(): TaxiDataCleanUtils = {
    newUtils(this.data.filter(col("lon") < lit(135.05) && col("lat") < lit(53.55) && col("lon") > lit(73.66) && col("lat") > lit(3.86)))
  }
  /**
    * 过滤定位失败的数据
    *
    * @return self
    */
  def filterStatus(): TaxiDataCleanUtils = {
    newUtils(this.data.filter(col("status") === lit("0")))
  }
}
