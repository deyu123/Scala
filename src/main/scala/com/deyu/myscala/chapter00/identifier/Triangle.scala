package com.deyu.myscala.chapter00.identifier

import scala.util.control.Breaks._

object Triangle {

  def main(args: Array[String]): Unit = {
    val rows = 10

//    for(int i =0;i<rows;i++) {
//      int number = 1;
//      //打印空格字符串
//      System.out.format("%"+(rows-i)*2+"s","");
//      for(int j=0;j<=i;j++) {
//        System.out.format("%4d",number);
//        number = number * (i - j) / (j + 1);
//      }
//      System.out.println();
//    }

//    for(i <- 0 to  rows) {
//      var number:Int = 1
//      print(" " * (rows-i))
//
//      for(j <- 0 to i){
//        print(raw"   $number")
//        number = number * (i - j )/ (j + 1)
//      }
//      println()
//    }
//

    var aa = for {i <- Range(1, 18, 2)
         j = (18 - i) / 2

    }yield {

      println(" "* j + "*"*i)

      i

    }

    var bb = for(i <- Range(1,18,2)) yield {

      print(i)
      i
    }

    val cc = for(i <- 1 to 10) yield i * 2

    println(aa)
    println(bb)
    println(cc)

    val c = 3

    breakable(

      if (c == 3){
        break
      }else{
        println("aaa")
      }
    )


  }


}
