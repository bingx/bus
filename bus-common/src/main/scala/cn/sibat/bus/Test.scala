package cn.sibat.bus

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

case class Test(id: String, num: String)

/**
  * hh
  * Created by kong on 2017/4/10.
  */
object Test {

  def main(args: Array[String]): Unit = {
    val data = Array("a,0", "b,5", "c,3", "d,0", "b,0", "a,0")
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/path/to/my").appName("t").master("local[*]").getOrCreate()
    import spark.implicits._
    val df = spark.sparkContext.parallelize(data).map(_.split(",")).toDF("id","num").as[Test]
    df.show()
    df.printSchema()
//      map(s => Test(s.split(",")(0), s.split(",")(1))).toDF()
//    val result = df.groupByKey(row => row.getString(row.fieldIndex("id"))).flatMapGroups((s, it) => {
//      it.map(row => row.mkString(","))
//    }).map(s => {
//      val split = s.split(",")
//      (split(0),split(1))
//    }).toDF("id","num")
//    println(result.isInstanceOf[Dataset[Test]])
//    result.show()
//    result.printSchema()

  }
}
