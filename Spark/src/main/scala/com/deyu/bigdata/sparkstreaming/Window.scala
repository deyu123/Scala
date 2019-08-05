package com.deyu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Window {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("worldCount")
    val context = new StreamingContext(conf, Seconds(3))

    val socket = context.socketTextStream("hadoop202", 9999)

    // 将采集周期当成一个整体来使用, 滑动的大小， 滑动的步长都应该是采集周期的整数倍
    val win = socket.window(Seconds(9), Seconds(3))
    val world = win.flatMap(_.split(" ").map((_,1))).reduceByKey(_+_)

    world.print()

    context.start()
    context.awaitTermination()


  }

}
