package cn.sibat.metro

/**
  * Created by wing1995 on 2017/4/20.
  */
object AnonymousFunction {
  val plusOne = (x: Int) => x + 1

  /**
    * Main method
    *
    * @param args application arguments
    */
  def main(args: Array[String]) {
    println(plusOne(0)) // Prints: 1
  }
}
