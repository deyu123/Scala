package com.deyu.myscala.vardemo

object VarValDemo02 {

  def main(args: Array[String]): Unit = {
    var dog1 = new Dog()
    dog1.name = "mary"
    println(dog1.name)
    dog1.name = null
    dog1.name = "tom"
    // 对象的地址是不可变的， 对象不可变， matt,val 认为得到对象只是改变他的值， 而不是改他的地址，
//    dog1 = null

    val dog2 = new Dog()
    dog2.name = "king"
    println(dog2.name)
//    dog2 = null
//    dog2 = dog1
//    println( dog2.name)
  }

}

class Dog {
  var name: String = "tom"
}