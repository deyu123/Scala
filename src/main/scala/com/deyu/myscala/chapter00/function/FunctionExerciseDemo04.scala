package com.deyu.myscala.chapter00.function

object FunctionExerciseDemo04 {
  def main(args: Array[String]): Unit = {
    def reduce(i:Int) = {

      def innerMethod(f:(Int, Int) => Int) = {
        def sum(x:Int, y:Int):Int = {
          if (y == 1){
            f(x, y)
          }else{
            f(x, sum(y, y -1))
          }
        }

        sum(i, i -1)
      }

      innerMethod _
    }


    println(reduce(5)(_+_))
  }

}
