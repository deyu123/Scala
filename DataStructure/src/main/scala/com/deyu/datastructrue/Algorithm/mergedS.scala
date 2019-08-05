package com.deyu.datastructrue.Algorithm

object mergedS {

  def mergedSort[T](less: (T, T) => Boolean)(list: List[T]): List[T] = {
    def merged(xList: List[T], yList: List[T]): List[T] = {
      (xList, yList) match {
        case (Nil, _) => yList
        case (_, Nil) => xList
        case (x :: xTail, y :: yTail) => {
          println("merged:" + x +":"+ y)
          if (less(x, y)) x :: merged(xTail, yList)
          else
            y :: merged(xList, yTail)
        }
      }
    }

    val n = list.length / 2
    if (n == 0) list
    else {
      val (x, y) = list splitAt n
      println(x +":"+ y)
      merged(mergedSort(less)(x), mergedSort(less)(y))
    }
  }


  def main(args: Array[String]) {
    val list = List(3, 12, 43, 23, 7, 1, 2, 0)
    println(mergedSort((x: Int, y: Int) => x < y)(list))
  }

}
