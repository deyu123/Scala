package com.deyu.myscala.chapter00.function

object FunctionDemo02 {

  def main(args: Array[String]): Unit = {
    def f1(): String = {
      return "hello scala"
    }

    // return 关键字
    def f2(): String = {
      "hello scala"
    }

    // 返回值类型 加 = ,自动推断
    def f3() = {
      "hello scala"
    }

    // 返回值类型 不加 = ， 默认为Unit
//    def f4() {
//      "hello scala"
//    }

    // 花括号也可以省略， =  就是自动类型推断
    def f5() = "hello scala"

    // 无参构造时,小括号可以省略
    def f6 = "hello scala"

    println(f1())
    println(f2())
    println("自动推断 :" + f3())
//    println("unit :" + f4())
    println("{} :" + f5())
    // 声明 和调用要统一格式， 声明没有加 () , 调用也不能加
    //    println("() :" + f6())
    println("() :" + f6)

  }

}
