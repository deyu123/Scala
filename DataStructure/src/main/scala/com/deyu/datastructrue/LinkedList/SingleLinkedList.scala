package com.deyu.datastructrue.LinkedList

object SingleLinkedList {

  def main(args: Array[String]): Unit = {
      // 头节点， 应为他来遍历
    val head = new HeroNode(-1, "" ,"")

    // 添加节点

    // 判断链表是否为 null
    def isEmpty():Boolean = {
      head.next == null
    }
    // 遍历


  }
}

// 都是带头节点的
class HeroNode(hNo:Int, hName : String, hNickName : String) {
  val no = hNo
  var name = hName
  var nickName = hNickName
  var next:HeroNode = null
}