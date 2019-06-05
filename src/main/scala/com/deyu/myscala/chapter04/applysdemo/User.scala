package com.deyu.myscala.chapter04.applysdemo

// 增加val 扩大作用域
class User (val s:String, val age: Int) {
    println("sssssss : "  + s)
    println("age : "  + age)
}

object  User{
  // 使用 apply 来构建对象
  def apply(s: String, age:Int): User = new User(s, age)

  def unapply(user: User): Option[(String, Int)] = {

    if(user == null){

      None
    }else{
      Some(user.s, user.age)
    }

  }

}

