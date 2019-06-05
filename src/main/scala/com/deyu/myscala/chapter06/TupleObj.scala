package com.deyu.myscala.chapter06

object TupleObj {
  def main(args: Array[String]): Unit = {

      val map1 = Map(("a", 1), ("b", 2),("c", 3))
      val map2 = Map(("a", 1), ("b", 2),("d", 3))
      // 模式匹配
      map1.foreach{
        case (k,v) => println(k + "=" + v)
      }

    var tup1 = 2 -> 11
    var tup2 = 0 -> 22
    var tup3 = 5 -> 33

    var tup4 = 6 -> 44 -> 55 // 结果是什么 两个tuple  ((6,44),55)

    println(tup4)




  }

}
