package com.deyu.myscala


object Holiday {

  def main(args: Array[String]): Unit = {
    var holidayNum:Int = 97
    val weekNum:Int = 7
    var week:Int = 0
    var day:Int = 0
    week = holidayNum / weekNum
    day = holidayNum % weekNum
    println("week : " + week)
    println("day : " + day)

    println((1000.0 / 3).formatted("%.2f"))
  }




}
