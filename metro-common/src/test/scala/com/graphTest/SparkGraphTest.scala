package com.graphTest

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.DataTypes

/**
  * 图测试
  * Created by wing1995 on 2017/5/26.
  */
object SparkGraphTest extends App{
  val spark = SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
    .appName("Spark Graph Test")
    .master("local[*]")
    .getOrCreate()
  //创建DataFrame
  val bikeStations = spark.read
    .format("csv")
    .option("header", "true")
    .csv("E:\\trafficDataAnalysis\\testData\\babs_open_data_year_2\\201508_station_data.csv")
  val tripData = spark.read
    .format("csv")
    .option("header", "true")
    .csv("E:\\trafficDataAnalysis\\testData\\babs_open_data_year_2\\201508_trip_data.csv")
  bikeStations.createOrReplaceTempView("sf_201508_station_data")
  tripData.createOrReplaceTempView("sf_201508_trip_data")
  //数据准备
  val justStations = bikeStations
    .withColumn("station_id", bikeStations("station_id").cast(DataTypes.IntegerType))
    .selectExpr("station_id", "name")
    .distinct()
  val completeTripData = tripData
    .join(justStations, tripData("Start Station") === bikeStations("name"))
    .withColumnRenamed("station_id", "start_station_id")
    .drop("name")
    .join(justStations, tripData("End Station") === bikeStations("name"))
    .withColumnRenamed("station_id", "end_station_id")
    .drop("name")

  import spark.implicits._
  val stations = completeTripData
    .select("start_station_id", "end_station_id")
    .rdd
    .distinct()
    .flatMap(x => Iterable(x(0).asInstanceOf[Number].longValue(), x(1).asInstanceOf[Number].longValue()))
    .distinct()
    .toDF()
  //创建vertices并给每一个节点添加标签数据
  val stationVertices: RDD[(VertexId, String)] = stations
    .join(justStations, stations("value") === justStations("station_id"))
    .select("station_id", "name")
    .rdd
    .map(row => (row(0).asInstanceOf[Number].longValue(), row(1).asInstanceOf[String]))
  stationVertices.take(2).foreach(println)
  //创建站点之间的边edge，并赋予初始权重1
  val stationEdge: RDD[Edge[Long]] = completeTripData
    .select("start_station_id", "end_station_id")
    .rdd
    .map(row => Edge(row(0).asInstanceOf[Number].longValue(), row(1).asInstanceOf[Number].longValue(), 1))
  val defaultStation = ("Missing Station")
  val stationGraph = Graph(stationVertices, stationEdge, defaultStation)
  stationGraph.cache()

  //println("Total Number of Station: " + stationGraph.numVertices)
  //println("Total Number of Trips: " + stationGraph.numEdges)
  //println("Total Number of Trips in Original Data: " + tripData.count())
  //PageRank重要程度排序（被连接次数排序）
//  val ranks = stationGraph.pageRank(0.0001).vertices
//  ranks
//    .join(stationVertices)
//    .sortBy(_._2._1, ascending = false) //sort by the rank
//    .take(10) //top 10
//    .foreach(x => println(x._2._2))
  //The most common OD routes
//  stationGraph
//    .groupEdges((edge1, edge2) => edge1 + edge2)
//    .triplets
//    .sortBy(_.attr, ascending = false)
//    .map(triplet =>
//    "There were " + triplet.attr.toString + " trips from " + triplet.srcAttr + " to " + triplet.dstAttr + ".")
//    .take(10)
//    .foreach(println)
  //inDegrees or outDegrees
//  stationGraph
//    .outDegrees
//    .join(stationVertices)
//    .sortBy(_._2._1, ascending = false)
//    .take(10)
//    .foreach(x => println(x._2._2 + " has " + x._2._1 + " outDegrees"))
  //A station were trips end at but rarely start from
  stationGraph
    .inDegrees
    .join(stationGraph.outDegrees)
    .join(stationVertices)
    .map(x => (x._2._1._1.toDouble/x._2._1._2.toDouble, x._2._2))
    .sortBy(_._1)
    .take(5)
    .foreach(x => println(x._2 + " has a in/out ratio of " + x._1))
}

