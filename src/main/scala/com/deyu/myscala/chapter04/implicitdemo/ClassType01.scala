package com.deyu.myscala.chapter04.implicitdemo

object ClassType01 {

  def main(args: Array[String]): Unit = {
    /**
      * 定义转换规则的函数
      */

    implicit def transform(d: Double): Int = {
      d.toInt
    }

    // 转换规则，不能有多个
    // implicit def transform1(d: Double): Int = {
    ////      d.toInt
    ////    }

    // 隐式参数转换 将Double => Int
    val i: Int = 3.5
    println(i)

    /**
      * 定义隐式转换的变量
      * 1. 遵循 ocp 开发原则 ，尽量不修改代码
      * 2. 有隐式转换的变量，就会优先使用隐式转换的变量
      *
      */

    implicit val testVar: String = "jack"

    def printx(implicit s: String = "tom"): Unit = {
      println(s)
    }

    printx("zhangSan")  // zhangsan
    printx  // jack



  }

}
