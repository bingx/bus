package cn.sibat.taxi

import java.text.SimpleDateFormat

import cn.sibat.taxi
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Lhh on 2017/5/12.
  */
class TaxiFunc(taxiDataCleanUtils: TaxiDataCleanUtils) extends Serializable{

  import taxiDataCleanUtils.data.sparkSession.implicits._

  /**
    * 出租车OD：/parastor/backup/datum/taxi/od/
    * @param taxiDealClean
    */
  def OD(taxiDealClean: Dataset[TaxiDealClean]):Unit = {
    //1.按车牌进行groupBy 2.根据time由小到大进行排序 3.将相连两条记录组合 4.过滤上、下车经纬度不全的数据 5.过滤距离小于300米的数据
    val result = taxiDataCleanUtils.data.join(taxiDealClean,"carId").toDF().groupByKey(row =>
  row.getString(row.fieldIndex("carId"))+row.getString(row.fieldIndex("upTime")))
      .flatMapGroups((s,it) => {
        val result = new ArrayBuffer[TaxiOD]()
        var upTime = ""
        var upTimePlus = ""
        var upDif = 30.0
        var downTime = ""
        var downTimePlus = ""
        var downDif = 30.0
        var distance = 0.0
        var upLon = 0.0
        var upLat = 0.0
        var downLon = 0.0
        var downLat = 0.0
        var carId = ""
        it.toArray.sortBy(row => row.getString(row.fieldIndex("time"))).foreach(row => {
          carId = row.getString(row.fieldIndex("carId"))
          val time1 = row.getString(row.fieldIndex("upTime"))
          val time2 = row.getString(row.fieldIndex("time"))
          val time3 = dealTime(time1,time2).abs
          if(time3 > 0 && time3 < 30 && time3 < upDif){
            upTime = time1
            upTimePlus = time2
            upDif = time3
            upLon = row.getDouble(row.fieldIndex("lon"))
            upLat = row.getDouble(row.fieldIndex("lat"))
          }
          val time4 = row.getString(row.fieldIndex("downTime"))
          val time5 = row.getString(row.fieldIndex("time"))
          val time6 = dealTime(time4,time5).abs
          if(time6 > 0 && time6 < 30 && time6 < downDif){
            downTime = time4
            downTimePlus = time5
            downDif = time6
            downLon = row.getDouble(row.fieldIndex("lon"))
            downLat = row.getDouble(row.fieldIndex("lat"))
          }
        })
        if(upDif != 30.0 && downDif != 30.0){
          distance = dealDistance(upLon,upLat,downLon,downLat)
        }
        if(distance > 300){
          val speed = distance/dealTime(upTime,downTime)
          val timeDif = dealTime(upTime,downTime)
          result += TaxiOD(carId,upTime,upTimePlus,upDif,upLon,upLat,downTime,downTimePlus,downDif,downLon,downLat,distance,speed,timeDif)
        }
        result
      }).filter("speed < 33.33")
    println(result.count())
    result.filter("timeDif < 180").show(10)
    println(result.filter("timeDif < 180").count())
    result.filter("timeDif < 300").show(10)
    println(result.filter("timeDif < 300").count())
    result.filter("timeDif < 480").show(10)
    println(result.filter("timeDif < 600").count())
//    result.rdd.saveAsTextFile("F:\\重要数据\\TaxiOD")
  }
  /**
    * 两个时间点之间的差值
    * @param firstTime
    * @param lastTime
    * @return 时间差
    */
  def dealTime(firstTime: String, lastTime: String): Double = {
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
  def dealDistance(lon1:Double,lat1:Double,lon2:Double,lat2:Double) : Double = {
    val earth_radius = 6367000
    val hSinY = Math.sin((lat1-lat2)*Math.PI/180*0.5)
    val hSinX = Math.sin((lon1-lon2)*Math.PI/180*0.5)
    val s = hSinY * hSinY + Math.cos(lat1*Math.PI/180) * Math.cos(lat2*Math.PI/180) * hSinX * hSinY
    2 * Math.atan2(Math.sqrt(s),Math.sqrt(1 - s)) * earth_radius

  }
}

/**
  * 出租车OD数据
  * @param carId
  * @param upTime
  * @param upTimePlus
  * @param upDif
  * @param upLon
  * @param upLat
  * @param downTime
  *
  * @param downTimePlus
  * @param downDif
  * @param downLon
  * @param downLat
  * @param distance
  */
case class TaxiOD(carId:String,upTime:String,upTimePlus:String,upDif:Double,upLon:Double,upLat:Double,downTime:String,
                  downTimePlus:String,downDif:Double,downLon:Double,downLat:Double,distance:Double,speed:Double,timeDif:Double)

object TaxiFunc {
  def apply(taxiDataCleanUtils: TaxiDataCleanUtils): TaxiFunc = new TaxiFunc(taxiDataCleanUtils)
}