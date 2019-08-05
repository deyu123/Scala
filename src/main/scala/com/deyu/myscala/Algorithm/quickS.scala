package com.deyu.myscala.Algorithm

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
        quickSort(left) ::: head :: quickSort(right)
    }
  }

  def main(args: Array[String]) {
    val list = List(1,2)
    println(quickSort(list))
  }

}
