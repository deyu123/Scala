package com.deyu.bigdata.sparkcore.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object combineByKey {
  def main(args: Array[String]): Unit = {
    // 创建配置项
    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local")
    // 得到spark 上下文
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a", 50), ("b", 20), ("c", 30), ("a",30)))
    val comRDD = rdd.combineByKey(
      x => (x, 1),
      (t: (Int, Int), x: Int) => {
        (t._1 + x, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      })

    val resultRDD: RDD[(String, Int)] = comRDD.map {
      case (key, (sum, count)) => {
        (key, sum / count)
      }
    }

    // 计算出一个个体
    val resCountRDD: RDD[(String, (Int, Int))] = rdd.mapValues((_,1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val resRDD: RDD[(String, Double)] = resCountRDD.mapValues(x => x._1.toDouble / x._2.toDouble)

    resRDD.collect().foreach(println)
    resultRDD.collect().foreach(println)


  }

}
