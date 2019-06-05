package com.deyu.myscala.chapter04.ArrayDemo

import scala.collection.mutable.ArrayBuffer

object ClassArray {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,2,3,4)

    /**
      *  输出打印的几种方式
      */
    for (elem <- arr) {
      println("11111111111elem: " + elem)
    }

    // 将Array 传递, 然后打印
    def prt(i:Int): Unit = {
      println("2222222222i:" + i)
    }
    arr.foreach(prt)
    //  x => println(x)
    arr.foreach(x=>println("3333333333x:" + x))

    arr.foreach(println(_))
    //    arr.foreach(println)

    // 初始化数组
    // array 没有[] , 只有()
    val arr1 = Array[Int](5, 6, 7, 8);
    arr1(0) = 1000
    println(arr1(0))
    println(arr1(2))
    //修改了原先的数组
//    println(arr1.update(0, 20))
    println(arr1(0))
    println(arr1.mkString("-"))

    // 增加了新的元素
    val ints = arr1:+6
    val intss = 5+:arr1
    println(intss.foreach(println))

    //删除， 必须改变位可变数组 ArrayBuffer
    val intArrayBuffer = ArrayBuffer(1,5,3,4)
    intArrayBuffer.remove(0)
    intArrayBuffer.insert(1,88,88, 88, 88)


    println(intArrayBuffer)

    //TODO 数组的转换

    val array = intArrayBuffer.toArray  // 可变 => 不可变
    val toBufferr1 = array.toBuffer     // 不可变 => 可变

  }

}
