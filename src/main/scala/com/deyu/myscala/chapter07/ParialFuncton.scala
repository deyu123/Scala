package com.deyu.myscala.chapter07

object ParialFuncton {
  /**
    * 先看一个需求
    *
    * 给你一个集合val list = List(1, 2, 3, 4, "abc") ，请完成如下要求:
    * 将集合list中的所有数字+1，并返回一个新的集合
    * 要求忽略掉 非数字 的元素，即返回的 新的集合 形式为 (2, 3, 4, 5)
    *
    * @param args
    */

  def main(args: Array[String]): Unit = {
    // 使用偏函数
    val list = List(1, 2, 3, 4, "abc")
    val ints = list.collect{case i:Int => i+1}
    println(ints)


    // 控制抽象
    def myRunInThread(f1: () => Unit) = {
      new Thread {
        override def run(): Unit = {
          f1()
        }
      }.start()
    }
    // 参数是函数， 函数没有输入值，也没有返回值
    // 可以将代码传入
    myRunInThread {
      () => println("干活咯！5秒完成...")
        Thread.sleep(5000)
        println("干完咯！")
    }

    // 控制抽象 实现递归
    var x = 10
    def until(condition: => Boolean)(block: => Unit): Unit = {
      //类似while循环，递归
      if (condition) {
        block
        until(condition)(block)
      }
      //      println("x=" + x)
      //      println(condition)
      //      block
      //      println("x=" + x)
    }

    until(x > 0) {
      x -= 1
      println("x=" + x)
    }



  }

}
