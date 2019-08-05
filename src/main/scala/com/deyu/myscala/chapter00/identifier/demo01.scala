package com.deyu.myscala.chapter00.identifier

object demo01 {

  def main(args: Array[String]): Unit = {

    // 运算符的两个不同点：
    var +- = "hello"
    println("+- " + +-)

    // Int 预定义的字符
    var Int = 100
    println("Int " + Int)

    println("------------")
    val flg: Boolean = false
    val result :Int = if (flg) {
      println("aa")
      1
    } else {
      println("bb")
//      0
      10
    }

    print(result)


  }
}
