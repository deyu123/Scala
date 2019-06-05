package com.deyu.myscala.chapter06

object methodObj {
  def main(args: Array[String]): Unit = {
    val list1 = List(1,2,3,4)
    val list2 = List(3,4,5,6)

    // 合集
    println(list1.union(list2))
    // 交集
    println(list1.intersect(list2))
    //
    println(list1.diff(list2))
    println(list2.diff(list1))
    // 拉链
    println(list1.zip(list2))
    // 滑动窗口 2, 每次滑动 2
    println(list1.sliding(2,2 ).foreach(println))
    //扫描， 将每次的结果记录下来
    println(list1.scan(0)(_+_))




  }

}
