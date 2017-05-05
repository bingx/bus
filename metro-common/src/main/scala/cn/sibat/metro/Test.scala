package cn.sibat.metro
import org.apache.spark.sql.SparkSession

/**
  * Created by wing1995 on 2017/4/20
  */

object Test {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:/file:E:/bus")
      .appName("Spark SQL Test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //导入隐式转换
    val ds = spark.read.text("E:\\bus\\metro-common\\src\\main\\resources\\testData").as[String]
    var df = DataFormatUtils.apply.trans_metro_SZT(ds)
    df = new DataCleanUtils(df).recoveryData.toDF
    df.show(100)
  }
}
