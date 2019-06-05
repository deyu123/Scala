package com.deyu.myscala.chapter00.function

object FunctionDemo01 {

  def main(args: Array[String]): Unit = {
//    var dog = new Dog()
//    println(dog.name)

    // 无参无返回值
    def f1(): Unit ={
      println("f11111111111")
    }

    // 无参有返回值
    def f2(): String = {
      return "f2222222"
    }

    // 有参无返回值

    def test(s: String): Unit = {
      println(s)
    }

    // 有参有返回值
    def f3(s: String): String = {

      return s
    }

    // 参数列表为多个 无返回值 name,age  方法逻辑  {} 代码块 ， trim

    def f4(name : String , age: Int) : String = {

      "name:" + name + ", age: " + age

    }

    test("aaaa")


    f1()
    println(f2())
    println(f3("hello" + "scala"))
    println(f4("zdy", 25))
  }

}
