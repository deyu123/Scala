package com.deyu.myscala.chapter00.function

import java.lang

object FunctionExerciseDemo03 {
  def main(args: Array[String]): Unit = {
    // 匿名函数
    () -> (println("----"))
    val f1 = () => (println("aaa"))

    // 函数的调用
    //    f1

    // 参数的传递
    val f2 = (s: String) => (println("hello" + s))

    def f3(f: (String) => Unit): Unit = {
      f("scala")
    }

    f3(f2)

    // 匿名函数作为返回值
    def f4() = {
      println("f444444444444")
    }

    def f5(): Unit = {
      //  将函数作为返回值
      //      f4
      //  函数不运行，当作返回值
      f4 _
    }

    // 函数调用
    println(f5())

    // 函数的柯里化
    def f6(i: Int)(j: Int) = {
      //将大的逻辑过程拆成一个个小的过程
      println((i * j))
    }

    // 调用
    //    f6(10)(20)

    // 函数的闭包, 改变了变量的生命周期
    def f7(a: String): (String => Unit) = {
      def f77(b: String): Unit = {
        println(a + b)
      }
      // 将其调用一下
      f77
      //      println(b)

    }

    f7("aaa")("bbb")

    // 体会闭包
    def makeAdd(more: Int) = (x: Int) => x + more

    def makeAddm(m: Int) = (x: Int) => x + m

    def normalAdd(a: Int, b: Int) = a + b

    val addOne = makeAdd(1)
    val addOnem = makeAddm(1)
    val addTwo = makeAdd(2)
    val normalTwo = normalAdd(2, 4)

    println(addOne(11))
    println(addOnem(222))
    println(addTwo(22))
    println(normalTwo)

    // trim

    def f8(a: String) = {
      println(a.trim)
    }
    f8("   bbdsafdsa  ")

    // 可变参数 不传递 List() , 传递的：WrappedArray(aa)
    def f9(b:String *): Unit ={
      println(b )
    }
    f9("aa")



  }

}
