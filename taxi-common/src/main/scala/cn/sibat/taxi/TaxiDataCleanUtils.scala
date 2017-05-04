package cn.sibat.taxi


import java.text.SimpleDateFormat

//import cn.sibat.bus.LocationUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
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
    * 过滤定位失败的数据
    *
    * @return self
    */
  def filterStatus(): TaxiDataCleanUtils = {
    newUtils(this.data.filter(col("status") === lit("0")))
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
    * 过滤车一整天所有点都为0.0,0.0的数据,局部经纬度为0.0，0.0不做过滤
    * 使用了groupByKey,很耗性能，如果局部经纬度为0.0，0.0没有影响的话
    * 使用 @link{ cn.sibat.bus.BusDataCleanUtils.zeroPoint() }
    *
    * @return
    */
  def allZeroPoint(): TaxiDataCleanUtils = {
    val result = this.data.groupByKey(row => row.getString(row.fieldIndex("upTime")).split("T")(0) + "," + row.getString(row.fieldIndex("carId")))
      .flatMapGroups((s, it) => {
        var flag = true
        val result = new ArrayBuffer[TaxiData]()
        it.foreach { row =>
          val lon_lat = row.getDouble(row.fieldIndex("lon")) + "," + row.getDouble(row.fieldIndex("lat"))
          if (!"0.0,0.0".equals(lon_lat)) {
            flag = false
          }
          val bd = TaxiData(row.getString(row.fieldIndex("carId")), row.getDouble(row.fieldIndex("lon"))
            , row.getDouble(row.fieldIndex("lat")), row.getString(row.fieldIndex("upTime"))
            , row.getString(row.fieldIndex("SBH")), row.getDouble(row.fieldIndex("speed"))
            , row.getDouble(row.fieldIndex("direction")), row.getDouble(row.fieldIndex("locationStatus"))
            , row.getDouble(row.fieldIndex("X")), row.getDouble(row.fieldIndex("SMICarid"))
            , row.getDouble(row.fieldIndex("carStatus")), row.getString(row.fieldIndex("carColor")))
          result += bd
        }
        if (!flag) {
          result
        } else {
          None
        }
      }).filter(_ != null).toDF()
    newUtils(result)
  }

  /**
    * 两个时间点之间的差值
    * @param firstTime
    * @param lastTime
    * @return
    */
  def dealTime(firstTime: String, lastTime: String): Long = {
    var result = -1L
    try {
      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      result = (sdf.parse(lastTime).getTime - sdf.parse(firstTime).getTime) /1000
    }catch {
      case e:Exception => e.printStackTrace()
    }
    result
  }

  /**
    * 计算两个经纬度之间的距离
    * @param lon1 经度1
    * @param lat1 纬度1
    * @param lon2 经度2
    * @param lat2 纬度2
    * @return 距离（m）
    */
  def distance(lon1:Double,lat1:Double,lon2:Double,lat2:Double) = {
    val earth_radius = 6367000
    if(lat1 != 0.0 && lon1 != 0.0 && lat2 != 0.0 && lon2 != 0.0){
      val hSinY = Math.sin((lat1-lat2)*Math.PI/180*0.5)
      val hSinX = Math.sin((lon1-lon2)*Math.PI/180*0.5)
      val s = hSinY * hSinY + Math.cos(lat1*Math.PI/180) * Math.cos(lat2*Math.PI/180) * hSinX * hSinY
      2 * Math.atan2(Math.sqrt(s),Math.sqrt(1 - s)) * earth_radius
    }
  }
  /**
    * 添加时间间隔与位移
    * 位移指的是两点间的球面距离，并非路线距离
    * 比如车拐弯了，
    * C++++++B
    * +
    * +
    * +
    * A
    * 那么位移就是AB之间的距离，并非AC+CB
    * 时间字段异常的话interval=-1,第一条记录为起点0，0.0
    * 结果在元数据的基础上添加两列interval,movement
    *
    * @return df(BusData,interval,movement)
    */
  def intervalAndMovement(): TaxiDataCleanUtils = {
    val target = this.data.groupByKey(row => row.getString(row.fieldIndex("upTime")).split("T")(0) + row.getString(row.fieldIndex("carId")))
      .flatMapGroups((s, it) => {
        val result = new ArrayBuffer[String]()
        var firstTime = ""
        var firstLon = 0.0
        var firstLat = 0.0
        it.toArray.sortBy(row => row.getString(row.fieldIndex("upTime"))).foreach(f = row => {
          if (result.isEmpty) {
            firstTime = row.getString(row.fieldIndex("upTime"))
            firstLon = row.getDouble(row.fieldIndex("lon"))
            firstLat = row.getDouble(row.fieldIndex("lat"))
            result.+=(row.mkString(",") + ",0,0.0")
          } else {
            val lastTime = row.getString(row.fieldIndex("upTime"))
            val lastLon = row.getDouble(row.fieldIndex("lon"))
            val lastLat = row.getDouble(row.fieldIndex("lat"))
            val standTime = dealTime(firstTime, lastTime)
            val movement = distance(firstLon, firstLat, lastLon, lastLat)
//            val movement = LocationUtil.distance(firstLon, firstLat, lastLon, lastLat)
            result.+=(row.mkString(",") + "," + standTime + "," + movement)
            firstTime = lastTime
            firstLon = lastLon
            firstLat = lastLat
          }
        })
        result
      }).map(line => {
      val split = line.split(",")
      Tuple18.apply(split(0), split(1), split(2), split(3), split(4), split(5)
        , split(6), split(7), split(8).toDouble, split(9).toDouble, split(10).toDouble, split(11)
        , split(12).toDouble, split(13).toDouble, split(14).toDouble, split(15).toDouble, split(16).toLong, split(17).toDouble)
    }).toDF("sysTime", "dataType", "term", "carId", "route", "subRoute", "company", "status", "lon"
      , "lat", "high", "upTime", "speed", "direct", "carSpeed", "mileage", "interval", "movement")
    newUtils(target)
  }

}
