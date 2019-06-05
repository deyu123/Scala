package com.deyu.myscala.chapter06

import scala.collection.mutable

object SteamObj {
  def main(args: Array[String]): Unit = {
    def numsForm(n: BigInt) : Stream[BigInt] = n #:: numsForm(n+1)
    val stream1 = numsForm(1)
    println(stream1)
    println(stream1.head)
    println(stream1.tail)
    println(stream1.tail.tail.tail)
//    println(stream1.tails.foreach(println))

    val result1 = (0 to 100).map{case _=>Thread.currentThread().getName}
    // 使用不同的线程， 速度快
    val result2 = (0 to 100).par.map{case _=>Thread.currentThread().getName}
    println(result1)
    println(result2)
  }

}
