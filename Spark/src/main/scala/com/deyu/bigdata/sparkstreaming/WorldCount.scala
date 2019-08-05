package com.deyu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WorldCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("worldCount")
    val context = new StreamingContext(conf, Seconds(3))

    val socket = context.socketTextStream("hadoop202", 9999)
    val world = socket.flatMap(_.split(" ").map((_,1))).reduceByKey(_+_)

    world.print()

    context.start()
    context.awaitTermination()


  }

}
