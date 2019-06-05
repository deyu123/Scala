package com.deyu.myscala.chapter04.implicitdemo

object ClassType02 {
  def main(args: Array[String]): Unit = {
    // 隐式转换类
    implicit def transform(mySQL: MySQL): newMySQL ={

      new newMySQL
    }
    // 当编译失败的时候， 开始隐式转换
    val mysql = new MySQL
    mysql.insert()
    mysql.update()

  }

}


class MySQL{
  def insert(): Unit = {
    println("insert date ... ")
  }
}

class newMySQL{
  def update(): Unit = {
    println("update date ... ")
  }
}

