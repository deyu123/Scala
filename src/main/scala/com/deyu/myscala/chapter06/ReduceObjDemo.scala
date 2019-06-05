package com.deyu.myscala.chapter06

object ReduceObjDemo {
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4)
    println(list.reduce(_ + _))
    // 1-2-3-4
    println(list.reduceLeft(_ - _))
    //2-3 | 1- (2 -3) = 2 | 1-(2-(3-4))
    println(list.reduceRight(_ - _))

    val listFl = list.foldLeft(0)(_+_)
    println("list Fold : " + listFl)

    println(list.fold(0)(_ + _))
    println(list.foldLeft(0)(_ - _))
    println(list.foldRight(0)(_ - _))



  }

}
