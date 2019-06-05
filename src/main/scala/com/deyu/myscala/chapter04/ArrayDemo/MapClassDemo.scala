package com.deyu.myscala.chapter04.ArrayDemo

object MapClassDemo {
  def main(args: Array[String]): Unit = {
    //声明的方式
    val map = Map("a" ->1 , "b" ->2 )
    val map1 = Map(("a",1), ("b",2) )

    // 预防 空指针的的情况， option(some, none) , getOrElse()
    // 取不到返回一个默认值
    val i = map.get("c").getOrElse(0)
    val j = map.get("b").getOrElse(0)
    println(i)
    println(j)
    // 遍历
    for (elem <- map) {
      // 对偶
      println(elem._1 + "=" + elem._2)
    }

    // 增加map 元素
    val strToMap = map+("k" -> 9)

    println(strToMap)
    val mapToUpdate = map.updated("a", 111111111)
    println(mapToUpdate)

  }

}
