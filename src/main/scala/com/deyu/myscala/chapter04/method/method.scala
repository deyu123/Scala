package com.deyu.myscala.chapter04.method

object method {

  def main(args: Array[String]): Unit = {
    val me = new methodExec("aa",11)
    me.printRect()
  }
}

// 主构造器
class methodExec (aa: String, bb: Int){
  println(aa)
  println(bb)

//  def this(name: String, id: Int){
//    this
//    println("111")
//  }
//
//  def this(name: String, id: String){
//    this
//    println("222")
//  }


//  def this(age: Int){
//    this
//  }

  def printRect(): Unit = {

    for (i <- 1 to 10) {

      for (j <- 1 to 8) {
        print("*")
      }
      println()
    }
  }

}
