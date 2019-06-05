package com.deyu.myscala.chapter04.ArrayDemo

object ExeObjDemo {
  def main(args: Array[String]): Unit = {
    val t1 = Array(1,3)
    val t2 = Array(2,4)
    val tuples = t1.zip(t2)
    println(tuples.toList)

    val strRange = for(i <- Range(1, 5)) yield {
      println(i)
      // 将返回值填入
      i
    }
    println(strRange)

//    生成一个list1，其中的数值是从1至100的整数，基于list1，生成list2，要求list2的数值是从1至100的偶数。
    var l = (1 to 100).filter(x=>{x%2==0})
    println(l)

//    使用模式匹配，实现，输入1，输出“red”;输入2，输出“green”输入3，输出“yellow”,并考虑匹配不成功的情况。

    def myCalc(s:String) = {
      s match {
        case "1" => println("red")
        case "2" => println("green")
        case "3" => println("yellow")
        case _ => println("未知")
      }
    }

    myCalc("0")


  }



}
