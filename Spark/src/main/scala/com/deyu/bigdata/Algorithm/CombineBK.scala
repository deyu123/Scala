package com.deyu.bigdata.Algorithm

import org.apache.spark.{SparkConf, SparkContext}

object CombineBK {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WorldCount")
    val sc = new SparkContext(conf)
    //    求每个key对应value的平均值
    val arr = Array(("a", 1), ("a", 3), ("b", 3), ("b", 5), ("c", 4))

    val rdd = sc.parallelize(arr)
    rdd.combineByKey (
      v => (v, 1),
      (acc: (Int, Int), newV:Int) => {
        (acc._1 + newV, acc._2 + 1)
      },
      (acc1: (Int, Int), acc2: (Int, Int)) => {
        (acc1._1 + acc2._1, acc2._2 + acc2._2)
      }
    )

  }

}
