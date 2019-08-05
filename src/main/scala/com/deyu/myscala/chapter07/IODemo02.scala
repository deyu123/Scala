package com.deyu.myscala.chapter07

import scala.io.Source

object IODemo02 {
  def main(args: Array[String]): Unit = {
    val strings = Source.fromFile("1.txt").getLines().toList
    println(strings)
  }

}
