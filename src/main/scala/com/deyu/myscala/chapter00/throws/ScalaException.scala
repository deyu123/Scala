package com.deyu.myscala.chapter00.throws

object ScalaException {

  def main(args: Array[String]): Unit = {

    try {
      val i = 1 / 0
    } catch {
      case ex: ArithmeticException =>{
        println("算术异常" + ex.getMessage)

      }
      case ex: Exception =>{
        println(ex.getMessage)
      }

    } finally {

      println("finally")
    }


  }

}
