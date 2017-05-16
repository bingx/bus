package cn.sibat.bus

/**
  * 弗雷歇距离工具
  * 用于度量两个轨迹曲线的相似度
  * Created by kong on 2017/5/16.
  */
case class Point(lon: Double, lat: Double)

object FrechetUtils {

  /**
    * 计算i，j的弗雷歇距离
    * 使用递归计算出最大距离
    * @param line1 轨迹1
    * @param line2 轨迹2
    * @param matrix 距离矩阵
    * @param i 轨迹1的第i个位置
    * @param j 轨迹2的第j个位置
    * @return
    */
  def calDistance(line1: Array[Point], line2: Array[Point], matrix: Array[Array[Double]], i: Int, j: Int): Double = {
    if (matrix(i)(j) > -1.0) {
      return matrix(i)(j)
    } else if (i == 0 && j == 0) {
      matrix(i)(j) = disP(line1(i), line2(j))
    } else if (i > 0 && j == 0) {
      matrix(i)(j) = math.max(calDistance(line1, line2, matrix, i - 1, j), disP(line1(i), line2(j)))
    } else if (i == 0 && j > 0) {
      matrix(i)(j) = math.max(calDistance(line1, line2, matrix, i, j - 1), disP(line1(i), line2(j)))
    } else if (i > 0 && j > 0) {
      matrix(i)(j) = math.max(math.min(math.min(calDistance(line1, line2, matrix, i - 1, j), calDistance(line1, line2, matrix, i - 1, j - 1))
        , calDistance(line1, line2, matrix, i, j - 1)), disP(line1(i), line2(j)))
    } else {
      matrix(i)(j) = Double.MaxValue
    }
    matrix(i)(j)
  }

  /**
    * 两点距离
    * @param p1 经纬度1
    * @param p2 经纬度2
    * @return
    */
  def disP(p1: Point, p2: Point): Double = {
    LocationUtil.distance(p1.lon, p1.lat, p2.lon, p2.lat)
  }

  /**
    * 两轨迹曲线的相似度
    * @param line1 轨迹1
    * @param line2 轨迹2
    * @return
    */
  def compareGesture(line1: Array[Point], line2: Array[Point]): Double = {
    var result = 0.0
    val matrix = new Array[Array[Double]](line1.length)
    for (i <- line1.indices) {
      for (j <- line2.indices) {
        val arr = new Array[Double](line2.length).map(_ - 1.0)
        matrix(i) = arr
      }
    }
    result = calDistance(line1, line2, matrix, line1.length - 1, line2.length - 1)
    result
  }

  def main(args: Array[String]) {
    val a = Array(1, 2, 3)
    val b = Array(4, 5, 6, 7)
    val matrix = new Array[Array[Double]](3)
    for (i <- a.indices) {
      for (j <- b.indices) {
        val c = new Array[Double](b.length).map(_ - 0.1)
        matrix(i) = c
      }
    }
    println(matrix.flatten.mkString(","))
  }
}
