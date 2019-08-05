package com.deyu.bigdata.sparkcore.accumulater

import org.apache.spark.{SparkConf, SparkContext}


object Broadcast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Broadcast").setMaster("local")
    val sc = new SparkContext(conf)
    val list = List((1,4), (2,8), (3,16), (4,18),(1,5))

    val broadcastList = sc.broadcast(list)

    val kvRDD = sc.parallelize(List((1,"a"),(2,"b"),(3,"c")))
    val joinRDD = kvRDD.map {

      case (k1, v1) => {
        var v: Any = null
        // 获取广播变量的值
        for ((k2, v2) <- broadcastList.value) {
          if (k1 == k2) {
            v = v2
          }
        }
        (k1, (v1, v))
      }
    }
    joinRDD.foreach(println)

  }

}
