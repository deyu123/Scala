package com.deyu.myscala.chapter04.ArrayDemo

object TupleClassDemo {
  def main(args: Array[String]): Unit = {

    val tuple = (1,"22", 222, 333, "33", "str")
    println(tuple._1)
    println(tuple.productElement(2))
  }

}
