package com.deyu.myscala.Algorithm

/**
  * 输入：(2 -> 4 -> 3) + (5 -> 6 -> 4)
  * 输出：7 -> 0 -> 8
  * 原因：342 + 465 = 807
  */

class ListNode(var _x: Int = 0) {
  var next: ListNode = null
  var x: Int = _x
}


object Demo01 {


  def addTwoNumbers(l1: ListNode, l2: ListNode): ListNode = {

    while (l1.next != null) {
      l1.x = l1.x * 10
    }
    while (l2.next != null) {
      l2.x = l2.x * 10
    }

    println(l1.x)
    println(l2.x)

    l1

  }

  def main(args: Array[String]): Unit = {

  }


}


