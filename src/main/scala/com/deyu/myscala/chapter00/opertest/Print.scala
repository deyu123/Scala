package com.deyu.myscala.chapter00.opertest

import scala.io.StdIn

object Print {
  def main(args: Array[String]): Unit = {

    var name : String = "zhangsan"
    var age : Int = 20
    var email : String = "zhangsan@163.com"

    //插值字符串 , s 变量直接可以使用
    println(s"name = $name")
//    println(f"age = $age%.2f")
    //原始数据层
    println(raw"email = $email")
    //换行
    print(
      s"""用户名称 = $name
         age = $age
         邮箱 = $email
       """)

    //声明变量 var 必须要初始化
    //val 声明的值没法改变
//    val age : Int = 30
//    age = 20

      // 能够自行推断语法值
    val password = "123123"


    val i : Int = 10
    val bt = i.toByte
    println(bt)

    val s = "123"
    val int = s.toInt
    println(int)

    println("------------------")

    var char: Char = 'a'

    println((char + 1).toChar)


    println("------------------")

    var r2 : Double = 10 / 3
    println(r2)

    println("------------------")
    val str = StdIn.readLine()
    println(str)




  }

}
