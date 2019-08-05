package com.deyu.bigdata.sparkcore.demo

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Key_Value_01 {
  def main(args: Array[String]): Unit = {
    //partition by
    val conf = new SparkConf().setAppName("WC").setMaster("local")
    val sc = new SparkContext(conf)
    val rddKV = sc.parallelize(List(("a", 3), ("a", 3), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 3)
    val rddKV1 = sc.parallelize(List(("a", 3), ("b", 3), ("c", 4), ("b", 3), ("c", 6), ("c", 8), ("d",9)), 3)

    val groupRDD = rddKV.groupByKey().map {
      case (key, datas) => {
        (key, datas.sum)
      }
    }
    //    groupRDD.collect().foreach(println)

    val reduceRDD = rddKV.reduceByKey(_ + _)
    //    reduceRDD.collect().foreach(println)
    // 分区内最大值相加
    val aggregateRDD = rddKV.aggregateByKey(0)(math.max(_, _), _ + _)
    //    aggregateRDD.collect().foreach(println)

    // 分区内和分区间规则相同
    val foldRDD = rddKV.foldByKey(0)(_ + _)
    //    foldRDD.collect().foreach(println)

    // 求平均值
    rddKV.combineByKey( // 初始值
      x => (x, 1),
      // 分区内规则  第一位相加， 第二位 +1
      (t: (Int, Int), x: Int) => {
        (t._1 + x, t._2 + 1)
      },
      // 分区间规则 , 将所有的进行汇总
      (t: (Int, Int), v: (Int, Int)) => {
        (t._1 + v._1, t._2 + v._2)
      }
    )


    // sortByKey  按照key 来排序
//    rddKV.sortByKey(false).collect().foreach(println)

    // mapValues案例

//    rddKV.mapValues(v=>{v + "|||"}).collect().foreach(println)


    // join 案例
    val joinRDD1 = rddKV1.join(rddKV)
    // 向上调用
    val joinRDD2 = rddKV1.cogroup(rddKV)
    joinRDD1.collect().foreach(println)
    joinRDD2.collect().foreach(println)
  }

}
