package com.deyu.myscala.chapter00.opertest

object Demo01 {

  def main(args: Array[String]): Unit = {
    var r1:Int = 10 / 3
    println("r1 10 / 3 :  "  + r1)
    var r2:Double = 10 / 3
    println("r2 :  "  + r2)
    var r3:Double = 10.0 / 3
    println("r3 :  "  + r3)
    // 使用四舍五入, 小数点后两位
    println("r3 :  "  + r3.formatted("%.2f"))

    var r4 = 10 % 3
    println("r4 : " + r4)

    var a:Double = 2
    var b:Double = 1.8
    println("a - b: " + (a-b))

    println("-------------")

    println("%4d", 4)

  }

}
