package com.deyu.myscala.structures.queue

import scala.collection.mutable

object QueueObj {
  def main(args: Array[String]): Unit = {
    val queue = new mutable.Queue[Int]
    queue += 1
    println(queue)
  }

}
