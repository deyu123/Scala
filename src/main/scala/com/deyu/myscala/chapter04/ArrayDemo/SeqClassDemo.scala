package com.deyu.myscala.chapter04.ArrayDemo

object SeqClassDemo {
  def main(args: Array[String]): Unit = {
    // 默认都是不可变
    val seqList = List(5,6, 7,8)
    // 空集合 Nil
    println(Nil)
    //::: 扁平化处理
    val ints = 1::2::3::4::seqList:::Nil
    println(ints)

  }
}
