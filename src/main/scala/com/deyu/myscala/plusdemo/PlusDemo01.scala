package com.deyu.myscala.plusdemo

object PlusDemo01 {
  def main(args: Array[String]): Unit = {
    var n1: Int = 1
    var c1: Char = 'a'
    println(n1.toString)
    println(c1.toString)

    var r1 = math.sqrt(3)
    println(r1)
    var r2 = r1 * r1
    println(r2)
    println(3-r2)
    println("crazy " * 3)
//    println(10 max(2))

    println(BigInt(2).pow(4))
    println("scala".charAt(0))
    println("scala".tail)

    println("scala".take(1))
    println("scala".takeRight(1))

  }
}
