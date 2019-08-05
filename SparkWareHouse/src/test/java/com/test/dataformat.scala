package com.test

import java.text.SimpleDateFormat
import java.util.Date

object dataformat {
  def main(args: Array[String]): Unit = {

      val time: Long = new SimpleDateFormat("yyyy-MM-dd").parse("2019-02-11").getTime

    val str: String = "1" match {
      case "1" => "PC"
      case "2" => "MOBILE"
      case "3" => "APP"
      case "4" => "WECHAT"
    }
    println(time)
  }
}
