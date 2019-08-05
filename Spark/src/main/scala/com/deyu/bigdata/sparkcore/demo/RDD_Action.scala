package com.deyu.bigdata.sparkcore.demo

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Action {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Action").setMaster("local")
    val sc = new SparkContext(conf)
    var rdd = sc.makeRDD(1 to 5, 2)

    //    val reduceRdd = rdd.reduce(_+_)
    // 长度
    //    val countRDD = rdd.count()

    //    val firstRdd = rdd.first()
    //    val toRDD = rdd.takeOrdered(3)
    //    val aggRDD = rdd.aggregate(10)(_+_,_+_)

    //    val foldRDD = rdd.fold(10)(_ + _)
    val mrdd = sc.makeRDD(List(("aa", 1),("aa", 2), ("bb", 2),("bb", 2), ("cc", 3), ("cc", 3),("dd", 4)))
    //    mrdd.saveAsTextFile("output1")
    //    mrdd.saveAsObjectFile("output2")

    // 计算key 的个数
    val cBV = mrdd.countByKey()
    println(cBV)

    // 直接遍历
    //    rdd.foreach(println)


  }

}
