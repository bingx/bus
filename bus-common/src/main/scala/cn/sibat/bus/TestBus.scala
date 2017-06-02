package cn.sibat.bus

import java.util.UUID

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class TestBus(id: String, num: String, or: String)

/**
  * hh
  * Created by kong on 2017/4/10.
  */
object TestBus {

  def main(args: Array[String]): Unit = {
    //    val field = Array("num","or")
    //    val sb = new StringBuilder()
    //    field.foreach(str => sb.append(s"$str == null ").append("&& "))
    //    println(sb.toString.substring(0,sb.lastIndexOf(" &&")))
    //    val data = Array("a,0,A", "b,5,B", "c,3,C", "d,0,D", "b,0,E", "a,0,F")
    //    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/path/to/my").appName("t").master("local[*]").getOrCreate()
    //    import spark.implicits._
    //    val df = spark.sparkContext.parallelize(data).map(s => (s.split(",")(0), s.split(",")(1), s.split(",")(2))).toDF("id", "num", "or").as[TestBus]
    //    //df.filter(sb.toString.substring(0,sb.lastIndexOf(" &&"))).show()
    //    df.filter(col("num") > 0 && col("num") < 5).show()

    //    var test: Row = null
    //    val data2 = Array("0,AA", "6,BB")
    //    val r = spark.sparkContext.parallelize(data2).map(s => (s.split(",")(0), s.split(",")(1))).toDF("num", "name")
    //    //df.join(r,df.col("num") === r.col("num")-3,"inner").show()
    //    val b = spark.sparkContext.broadcast(df.collect())
    //    r.collect().foreach(s => {
    //      if (test == null) {
    //        test = s
    //      }
    //      val re = b.value.filter(tb => tb.num.equals(s.getString(s.fieldIndex("num"))))
    //      println(re.length, re.mkString(","))
    //      println(test.mkString(";"))
    //    })
    //map模糊查询
    //    val map = Map("M2143"-> "kk","M2133"->"oo")
    //    var k:String = "null"
    //    map.keySet.foreach{key=>
    //      if(key.contains("M214"))
    //        k = key
    //    }
    //    println(map.getOrElse(k,"55"))
    //    val theta1 = (0.0 to math.Pi / 2 by 0.01)
    //    val theta2 = (0.0 to math.Pi / 2 by 0.01)
    //    val p1 = (0.0 to 1 by 0.01)
    //    val max2 = Array(Double.MaxValue,Double.MinValue)
    //    theta1.foreach { t1 =>
    //      theta2.foreach { t2 =>
    //        p1.foreach { p_1 =>
    //          val p_2 = 1.0 - p_1
    //          if ((p_1*math.cos(t2)+p_2*math.cos(t1))/(math.cos(t1)*math.cos(t2)) < 1.2){
    //            val dis = math.tan(t1)*p_1
    //            if (max2(0) > dis)
    //              max2(0) = dis
    //            if (max2(1) < dis)
    //              max2(1) = dis
    //            //println(t1,t2,p_1,p_2)
    //          }
    //        }
    //      }
    //    }
    //    println(max2.mkString(","))

//    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/path/to/my").appName("T").master("local[*]").getOrCreate()
    //    val rdd = spark.sparkContext.parallelize(0 to 100)
    //    rdd.saveAsTextFile("D://testData/test/textFile")
    //    rdd.saveAsObjectFile("D://testData/test/objectFile")
    //    import spark.implicits._
    //    val df = rdd.toDF()
    //    df.write.parquet("D://testData/test/parquet")
    //    df.map(_.toString()).write.text("D://testData/test/text")
    //    df.write.option("compression","gzip").csv("D://testData/test/csvWithGzip")
    //    df.write.json("D://testData/test/json")
    //    df.write.option("compression","bzip2").csv("D://testData/test/csvWithBzip2")
    //    //df.write.option("compression","lz4").csv("D://testData/test/csvWithLz4")
    //    //df.write.option("compression","snappy").csv("D://testData/test/csvWithSnappy")
    //    df.write.csv("D://testData/test/csv")
    //df.write.saveAsTable("table")
    //    spark.read.table("table").show()
//    import spark.implicits._
//    val data = spark.read.textFile("D:/testData/公交处/data/2016-12-01/*/*")
//    val filter = data.filter(s=>s.split(",").length > 16)
//    println(filter.map(_.split(",")(3)).distinct().count())
//    filter.rdd.sortBy(s=>s.split(",")(3)).repartition(1).saveAsTextFile("D:/testData/公交处/arrivalTime")
    val v1 = "2016-12-01T16:35:39.000Z,01,��BX4675,��BX4675,B6624,B6624,2,0,113.941452,22.754307,0.000000,2016-12-01T16:35:32.000Z,33.000000,314.000000,33.000000,0.000000,G_GM0798,2016-12-01T16:35:32.000Z,"
    val v2 = "2016-12-01T15:05:11.000Z,01,��BX4675,��BX4675,B6624,B6624,2,0,113.936195,22.758846,0.000000,2016-12-01T15:05:05.000Z,29.000000,313.000000,29.000000,0.000000,,2016-12-01T15:05:05.000Z,G_GM0195"
    println(v1.split(",")(19)+"\n"+v2.split(",")(16).isEmpty)
  }
}
