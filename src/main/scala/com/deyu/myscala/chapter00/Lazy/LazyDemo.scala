package com.deyu.myscala.chapter00.Lazy

object LazyDemo {

  def sum(i: Int, i1: Int): Int = {
    println("sum  被调用  ... ")
    i + i1
  }

  def main(args: Array[String]): Unit = {

    lazy val i = sum(1, 3)

    println("--------------")

    println("i = " + i)

  }

}
