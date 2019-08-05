package com.deyu.myscala.chapter07

/**
  *  样例类
  *  通过模式匹配的方式优化这个类
  *
  *
  */

object AmountDemo01 {

  def main(args: Array[String]): Unit = {

    for (amt <- Array(Dollar(1000.0), Currency(1000.0, "RMB"), NoAmount)) {
      val result = amt match {
        case Dollar(v) => "$" + v
        case Currency(v, u) => v + " " + u
        case NoAmount => ""
      }
      println(amt + ": " + result)
    }

    var list = List(1, 3, 5, 9)
    list match {
      case first :: second :: rest => println(first + "--" + second + "--" + rest)
      case _ => println("匹配不到...")
    }

  }

}

abstract class Amount
  case class Dollar(value: Double) extends Amount
  case class Currency(value: Double, unit: String) extends Amount
  case object NoAmount extends Amount


