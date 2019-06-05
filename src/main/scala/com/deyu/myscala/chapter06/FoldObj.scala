package com.deyu.myscala.chapter06

import scala.collection.mutable

object FoldObj {
  def main(args: Array[String]): Unit = {
    val map1 = mutable.Map("a" -> 1, "b" -> 3, "c" -> 5)
    val map2 = mutable.Map("a" -> 2, "b" -> 2, "d" -> 2)

    // map2 初始化队列,都放在初始队列中
    val stringToInt = map1.foldLeft(map2) {
      // map 第一个队列
      // kv 第二个队列
      (map, kv) => {
        // 更新 map 中的值
        val k = kv._1
        val v = kv._2
        map(k) = map.getOrElse(k, 0) + v
        // 返回 一个map
        map
      }
    }
    println(stringToInt)
  }

}
