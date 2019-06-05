package com.deyu.myscala.chapter00.function

object FunctionDemo04 {

  def main(args: Array[String]): Unit = {
    def test1(x: Double):(Double) => Double = {
      // 不能放在外面,有一个闭包的概念
      def test2(y: Double):Double= {
        x * x * y
      }

      // 不是调用，而是使用一个返回值
      //      test2 _
      test2
    }


    def test3(x: Double) = {
      // 不能放在外面,有一个闭包的概念
      def test4(y: Double)= {
        x * x * y
      }

      // 不是调用，而是使用一个返回值
      //      test2 _
      test4 _
    }

    val res1 = test1(2.0)(3.0)
    val res2 = test3(4.0)(3.0)
    println("res = " + res1)
    println("res = " + res2)
  }
}
