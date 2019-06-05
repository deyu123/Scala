package com.deyu.myscala.chapter04.ArrayDemo

object SetClassDemo {
  def main(args: Array[String]): Unit = {
    val ints = Set(1,2,3,4,5,6,2,3,4,5,6)
    println(ints)

    println(ints - 2)
    // ++, --  加减一个集合

  }

}
