package com.deyu.datastructrue.Algorithm

object quickS {

  /**
    * 快排
    * 时间复杂度:平均时间复杂度为O(nlogn)
    * 空间复杂度:O(logn)，因为递归栈空间的使用问题
    */

  def quickSort(list: List[Int]): List[Int] = {
    list match {
      case List() => List()
      case head :: tail =>
        val (left, right) = tail.partition(_ < head)
        println(left, right)
        quickSort(left) ::: head :: quickSort(right)
    }
  }

  def main(args: Array[String]) {
    val list = List(1,2,10, 8, 2, 22, 32, 2, 2, 34, 2, 4,76,78, 9, 9 ,6, 456)
    println(quickSort(list))
  }

}
