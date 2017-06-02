package cn.sibat.bus

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by kong on 2017/6/1.
  */
object BaoAnDemo {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("BaoAnDemo").master("local[*]").getOrCreate()
    //flowinst
    val d = spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/flowinst/*.sql")
    val flowinst = sqlToDataFrame(d)
    val flowreord = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/flowreord/*.sql"))
    val dingdianxunchafujian = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/JH_DingDianXunChaFuJian/*.sql"))
    val shijianfujian = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/JH_ShiJianFuJian/*.sql"))
    val shijianjiaohuan = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/JH_ShiJianJiaoHuan/*.sql"))
    val J_wanggeshuju = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/JH_WangGeShuJu/*.sql"))
    val wanggeyuan = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/Jh_WangGeYuan/*.sql"))
    val J_dingdianxuncha = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/JH_Wj_DingDianXunCha/*.sql"))
    val anjianqiyexinxi = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/PRM_AnJianQiYeXinXi/*.sql"))
    val P_dingdianxuncha = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/PRM_DingDianXunCha/*.sql"))
    val shequ = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/PRM_SheQu/*.sql"))
    val shixiangcanshu = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/PRM_ShiXiangCanShu/*.sql"))
    val street = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/PRM_Street/*.sql"))
    val xiaofangzhongdiandanwei = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/PRM_XiaoFangZhongDianDanWei/*.sql"))
    val changfanggaigongyu = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_ChangFangGaiGongYu/*.sql"))
    val guanliduixiang = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_GuanLiDuiXiang/*.sql"))
    val loudongxiangduanrenyuan = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_LouDongXiangGuanRenYuan/*.sql"))
    val loudongxinxi = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_LouDongXinXi/*.sql"))
    val wanggequanxian = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_WangGeQuanXian/*.sql"))
    val T_wanggeshuju = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_WangGeShuJu/*.sql"))
    val wanggeyuanpaibanbiao = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_WangGeYuanPaiBanBiao/*.sql"))
    val wanggeyuanqiandaobiao = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_WangGeYuanQianDaoBiao/*.sql"))
    val xiaofangnaguanrenwu = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_XiaoFangNaGuanRenWu/*.sql"))
    val xiaofangnaguanrenwujianchabiao = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_XiaoFangNaGuanRenWuJianChaBiao/*.sql"))
    val xiaofangnaguanrenwujianchabiaoxiangmu = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_XiaoFangNaGuanRenWuJianChaBiaoXiangMu/*.sql"))
    val xiaofangnaguanrenwumingxi = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_XiaoFangNaGuanRenWuMingXi/*.sql"))
    val xiaofangnaguanrenyuan = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_XiaoFangNaGuanRenYuan/*.sql"))
    val xiaofangnaguanyihuan = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_XiaoFangNaGuanYiHuan/*.sql"))
    val zhangjie = sqlToDataFrame(spark.read.textFile("D:/testData/宝安区/网格第一季度数据库表/TBD_ZhangJie/*.sql"))
    //未办结A、B级事件的街道分布
    //P_dingdianxuncha.where(col("XunChaJieGuo")==="未整治" && col("RenWuLaiYuan") === "A类事件").select("RenWuLaiYuan","street","XunChaJieGuo").groupBy(col("street")).count().show()
//    val row = P_dingdianxuncha.take(1)(0)
//    val id = row.getString(row.fieldIndex("LaiYuanID"))
//    println(row)
//    flowinst.select("ZhengZhiShiChang","ABCType","time","street","state")show(10)
    shixiangcanshu.show(10)
    println(P_dingdianxuncha.count())
    println(flowinst.count())
  }

  /**
    * 处理宝安的sql数据表
    */
  def sqlToDataFrame(sqlData: Dataset[String]): DataFrame = {
    import sqlData.sparkSession.implicits._
    val data = sqlData.filter(_.split("VALUES").length > 1)
    val schema = data.take(1)(0).split("VALUES")(0)
    val cols = schema.substring(schema.indexOf("(")).replaceAll("[\\]\\[()]", "").split(",").map(_.trim)
    val ds = data.map { s =>
      val split = s.split("VALUES")
      split(1).replaceAll("[(');]", "").split(",").map(_.trim).mkString(",")
    }.filter(s => s.split(",").length == cols.length)
    DataFrameUtils.apply.default2Schema(ds, cols: _*)
  }
}
