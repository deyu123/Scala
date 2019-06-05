package com.deyu.myscala.chapter04.applysdemo

import scala.beans.BeanProperty

object object1 extends App {


  @BeanProperty
  var aa: String = "aa"

  setAa("bbbbb")
  println(getAa)

  // 默认的public 设置为set,get 方法
  // protected 只有同类，子类可以使用，跟 Java 同类同包子类不同，
  // 可以指定包,只有objt 这个包可以访问
  private[applysdemo] def packObj: Unit = {
  }

  //调用 User 对象 , 不需要关键字
  User.apply("apply", 222)
  val user = User("test unapply", 111)
  // 传递一个对象， 返回构造器的值, 模式匹配
  user match {
      // 匹配上之后就不会走下面了
//    case User("test unapply",num) => println("age----" + num )
    case User(s,111) => println("str ------" + s)
    case _ => println("Not unapply"  )
  }

}

