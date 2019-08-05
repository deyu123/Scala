package com.deyu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStream {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("worldCount")
    val context = new StreamingContext(conf, Seconds(3))

    val socket = context.socketTextStream("hadoop202", 9999)

    // 更新状态操作时，更新检查点的信息
    context.sparkContext.setCheckpointDir("cp")
    val world = socket.flatMap(_.split(" ").map((_,1))).updateStateByKey[Int]{
      (seq:Seq[Int], opt:Option[Int]) => {
        val sum = opt.getOrElse(0) + seq.sum
        Option(sum)
      }
    }

    world.print()

    context.start()
    context.awaitTermination()


  }

}
