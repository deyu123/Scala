package com.deyu.myscala.chapter07

object Generic {
  def main(args: Array[String]): Unit = {
    // 测试 协变， 逆变， 不可变
//     val test:Test[User1] = new Test[Person1]()
    // 测试类型
     test(new Child1)
    //

  }

  // 类型约束
//  def test[T](t:T) ={
//    println(t)
//  }
  // 泛型的下线， 没有下线的概念，可以传入任何类
//  def test[T >: User1](t:T) ={
//    println(t)
//  }

  // 泛型的上线
  def test[T <: User1](t:T) ={
    println(t)
  }

}
class Person1{

}
class User1 extends Person1{

}
class Child1 extends User1{

}
// + :当前类及其子类
// - : 当前类和父类
class Test[-User1]{

}