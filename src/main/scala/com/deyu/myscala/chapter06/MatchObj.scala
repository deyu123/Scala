package com.deyu.myscala.chapter06

object MatchObj {
  def main(args: Array[String]): Unit = {
    val oper = '#'
    val n1 = 20
    val n2 = 10
    var res = 0
    oper match {
      case '+' => res = n1 + n2
      case '-' => res = n1 - n2
      case '*' => res = n1 * n2
      case '/' => res = n1 / n2
      case _ => println("oper error")
    }
    println("res=" + res)


    val a = 1
    val obj = if(a == 1) 1
    else if(a == 2) "2"
    else if(a == 3) BigInt(3)
    else if(a == 4) Map("aa" -> 1)
    else if(a == 5) Map(1 -> "aa")
    else if(a == 6) Array(1, 2, 3)
    else if(a == 7) Array("aa", 1)
    else if(a == 8) Array("aa")

    println(obj)

    // 后面的都没法走到
    val result = obj match {
      case a : Int => a
      case _ : BigInt => Int.MaxValue
      case b : Map[String, Int] => "一个字符串-数字的Map集合"
      case c : Map[Int, String] => "一个数字-字符串的Map集合"
      case d : Array[String] => "一个字符串数组"
      case e : Array[Int] => "一个数字数组"
      case _ => "啥也不是"
    }
    println(result)

    for (arr <- Array(Array(0), Array(1, 0), Array(0, 1, 0),
      Array(1, 1, 0), Array(1, 1, 0, 1))) {
      val result = arr match {

        case Array(0) => "0"
        case Array(x, y) => x + "=" + y
        case Array(0, _*) => "以0开头和数组"
        case _ => "什么集合都不是"
      }
      println("result = " + result)

    }

    for (list <- Array(List(0), List(1, 0), List(0, 0, 0), List(1, 0, 0))) {
      val result = list match {
        case 0 :: Nil => "0" //
        case x :: y :: Nil => x + " " + y
        case 0 :: tail => "0 ..."
        case _ => "something else"
      }
      println(result)
    }

    println("---------------------------")

    for (pair <- Array((0, 1), (1, 0), (2, 1),(1,0,2))) {
      val result = pair match {
        case (0, _) => "0 ..."
        case (y, 0) => y
        case (a,b) => (b,a)
        case _ => "other"
      }
      println(result)
    }




  }

}
