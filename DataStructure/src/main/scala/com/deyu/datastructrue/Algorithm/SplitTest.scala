package com.deyu.datastructrue.Algorithm

import scala.collection.mutable.ListBuffer

object SplitTest {
  def main(args: Array[String]): Unit = {
    val  str = "abc,def.ghi"
    val format =Array("ef", "c")

    println(fileSplit(str, format))

  }

  def fileSplit(str:String, format:Array[String]:ListBuffer[String] = {
    val list= new ListBuffer[String]()
    val first: Array[String] = str.split(format(0))
    first(0).split(format(1)).foreach(list+=_)
    list+= first(1)

  }

}
