package com.deyu.bigdata.sparkcore.core

import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {
  def main(args: Array[String]): Unit = {
    // 创建配置项
    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local")
    // 得到spark 上下文
    val sc = new SparkContext(conf)

    //    使用6种方式将下面的数据统计单词出现的次数
    val kvRDD = sc.makeRDD(List(("Hello Spark", 4), ("Hello Scala", 5),
      ("Spark Scala", 4), ("Hello Scala", 6), ("Hello Spark", 5), ("Scala Spark", 6)))

    // map
    val value1 = kvRDD.map(t => {
      (t._1 + " ") * t._2
    }).flatMap(x => x.split(" ")).groupBy(x => x).map(x => (x._1, x._2.size))

    // reduceByKey
    val value2 = kvRDD.flatMap {
      x => {
        val worlds = x._1.split(" ")
        // list 对偶
        worlds.map(y => (y, x._2))
      }
    }.reduceByKey(_ + _)

    // mapValues
    val value3 = kvRDD.flatMap {
      x => {
        val worlds = x._1.split(" ")
        // list 对偶
        worlds.map(y => (y, x._2))
      }
    }.groupBy(_._1).mapValues {
      x => {
        // 只关心 value , 不关心 key
        x.map(_._2).sum
      }
    }

    // aggregateByKey
    val value4 = kvRDD.flatMap {
      x => {
        val worlds = x._1.split(" ")
        // list 对偶
        worlds.map(y => (y, x._2))
      }
    }.aggregateByKey(0)(_ + _, _ + _)

    // foldByKey
    val value5 = kvRDD.flatMap {
      x => {
        val worlds = x._1.split(" ")
        // list 对偶
        worlds.map(y => (y, x._2))
      }
    }.foldByKey(0)(_ + _)

    // combineByKey
    val value6 = kvRDD.flatMap {
      x => {
        val worlds = x._1.split(" ")
        // list 对偶
        worlds.map(y => (y, x._2))
      }
    }.combineByKey(
      x => (x,1),
      (t: (Int, Int), v: Int) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    ).map{
      datas => {
        (datas._1,datas._2._1)
      }
    }
    value6.collect().foreach(println)

  }

}
