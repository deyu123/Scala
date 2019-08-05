package com.deyu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TextFileStream {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("worldCount")
    val context = new StreamingContext(conf, Seconds(3))

    // 只有当新建文件的时候才会
    val socket = context.textFileStream("in")
    val world = socket.flatMap(_.split(" ").map((_,1))).reduceByKey(_+_)

    world.print()

    context.start()
    context.awaitTermination()


  }

}
