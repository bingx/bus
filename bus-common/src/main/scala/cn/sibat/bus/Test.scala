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
    val data = Array("a,0,A", "b,5,B", "c,3,C", "d,0,D", "b,0,E", "a,0,F")
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/path/to/my").appName("t").master("local[*]").getOrCreate()
    import spark.implicits._
    val df = spark.sparkContext.parallelize(data).map(s => (s.split(",")(0), s.split(",")(1), s.split(",")(2))).toDF("id", "num", "or").as[TestBus]

    var test: Row = null
    val data2 = Array("0,AA", "6,BB")
    val r = spark.sparkContext.parallelize(data2).map(s => (s.split(",")(0), s.split(",")(1))).toDF("num", "name")
    //df.join(r,df.col("num") === r.col("num")-3,"inner").show()
    val b = spark.sparkContext.broadcast(df.collect())
    r.collect().foreach(s => {
      if (test == null) {
        test = s
      }
      val re = b.value.filter(tb => tb.num.equals(s.getString(s.fieldIndex("num"))))
      println(re.length, re.mkString(","))
      println(test.mkString(";"))
    })

  }
}
