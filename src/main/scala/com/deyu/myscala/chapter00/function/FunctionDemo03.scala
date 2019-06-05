package com.deyu.myscala.chapter00.function

import java.util

object FunctionDemo03 {
  def main(args: Array[String]): Unit = {

    //    () => {}
    //      () -> {}

    val f = (s: String) => {println("s = " + s)}
    () -> { println("----------")}
    f("scala")


    def f1(s: String): Unit = {
      println("f1 ... " + s)
    }


    def f2(f:(String) => Unit) = {
      f("scala ")
    }

    f2(f1)


    println("------------------------------------------------------")
    def f3() = {
      println("f3 ...")
    }

    def f4() = {
      // 不希望被调用， 而是希望做为返回值
      f3 _
    }
    // 调用
    f4()()

    println("------------------------------------------------------")

    def f5() = {

      def f55(i: Int) = {
        println("i = " + i)
      }

      f55 _
    }

    f5()(10)
    println("------------------------------------------------------")
    // 函数的柯里化
    def f66 (i:Int, j:Int) = {

      println((i * j))
    }

    def f6(j: Int)(i:Int) = {
      println(i * j)
    }

    f6(2)(10)


    println("函数闭包------------------------------------------------------")
    def f7(i: Int) = {
      def f77 (j: Int) = {
        println(i * j)
      }

      f77 _
    }

    f7(10)(29)


    println("匿名函数------------------------------------------------------")
    def f8(f:()=>Unit): Unit = {
      f()
    }

    def f88() = {
      println("f888888888")
    }

    def f888() = {
      println("xxxxxxxxxx")
    }

    val ff = (x:Int) => {println(x)}
    ff(10)

    f8(() => {println("xxx")})
//    f8(println("xxx"))
    f8(println)

    println("匿名函数反推------------------------------------------------------")
//    _.toInt + _.toInt


    println("------------------------------------------------------")

    // 调用函数
    f2(f1)

    def f9(i : Int) = {

      def f99 (j : Int) = {
        def f999 ( f : (Int, Int) => Int ) = {

          f(i, j)
        }

        f999 _
      }

      f99 _
    }

    val result: Int = f9(10)(20)((x, y) => {x + y})
    println(result)


  }

}
