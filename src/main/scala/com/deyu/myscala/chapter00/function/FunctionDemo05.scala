package com.deyu.myscala.chapter00.function

object FunctionDemo05 {

  def main(args: Array[String]): Unit = {


    def reduce(i: Int) = {
      var aa = 0
      def bb(f: (Int, Int) => Int) = {

        for(x <- 1 to i){
          aa = f(aa,x)
        }
        aa
      }
      bb _
    }

    def reduces(i: Int) = {
      var aa = 0
      def bb(f: (Int, Int) => Int) = {
        for(x <- Range(0,i)){
          aa = f(aa,x)
        }
        aa
      }
      bb _
    }

//    val rd= reduce(5)((x, y) => {x + y})
    val rd= reduce(5)(_ + _)
    println(rd)

  }

}
