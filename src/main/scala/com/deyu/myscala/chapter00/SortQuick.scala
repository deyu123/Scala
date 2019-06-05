package com.deyu.myscala.chapter00

import scala.util.control.Breaks._

object SortQuick {

  def quickSort(left: Int, right: Int, arr: Array[Int]): Unit = {

    var l: Int = left
    var r: Int = right
    var pivot = arr((left + right) / 2)
    var temp = 0

    //Array(10, 11, 2, -1, 3)
    breakable {
      while (l < r) {

        //从左点向右遍历，直到找到比中间值大的
        while (arr(l) < pivot) {
          l += 1
        }

        //从右点向左遍历，直到找到比中间值小的
        while (arr(r) > pivot) {
          r -= 1
        }

        //判断是否已经越过中间值
        if (l >= r) {
          break()
        }

        //交换数据
        temp = arr(l)
        arr(l) = arr(r)
        arr(r) = temp
      }
    }

    if (l == r) {
      l += 1
      r -= 1
    }

    //向左递归
    if (left < r) {
      quickSort(left, r, arr)
    }

    //向右递归
    if (right > l) {
      quickSort(l, right, arr)
    }

  }


  def quick_Sort(list: List[Int]): List[Int] = {
    list match {
      case Nil => Nil
      case List() => List()
      case head :: tail =>
        val (left, right) = tail.partition(_ < head)
        quick_Sort(left) ::: head :: quick_Sort(right)
    }
  }


  def main(args: Array[String]) {
//    val list = List(3, 12, 43, 23, 7, 1, 2, 0)
    var arr = Array(3, 12, 43, 23, 7, 1, 2, 0)
    println(for (elem <- arr) {print(elem + " ")})
    quickSort(0, 7, arr)
    println(for (elem <- arr) {print(elem + " ")})
//    println(quick_Sort(list))
  }

}
