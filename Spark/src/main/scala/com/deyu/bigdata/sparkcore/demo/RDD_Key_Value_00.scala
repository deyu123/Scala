package com.deyu.bigdata.sparkcore.demo

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RDD_Key_Value_00 {
  def main(args: Array[String]): Unit = {
    //partition by
    val conf = new SparkConf().setAppName("WC").setMaster("local")
    val sc = new SparkContext(conf)
    //    val rdd1 = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)
    //    println(rdd1.partitions.size)

    //    val rdd2 = rdd1.partitionBy(new org.apache.spark.HashPartitioner(2))
    //    println(rdd2.partitions.size)
    //    val rddKV = sc.parallelize(List(("a", 3), ("a", 3), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    val rddKV = sc.parallelize(List(("a", 3), ("a", 3), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 3)
    val rddKV1 = sc.parallelize(List(
      ("13909029812", ("20170507", "http://www.baidu.com")), ("18089376778", ("20170401", "http://www.google.com")), ("18089376778", ("20170508", "http://www.taobao.com")), ("13909029812", ("20170507", "http://www.51cto.com"))
    ), 2)
    // 两两之间比对 ， 每个分区的最大值， 然后相加
    val aggregateRDD = rddKV.aggregateByKey(0)(math.max(_, _), _ + _)
    //    aggregateRDD.collect().foreach(println)

    // aggregateByKey 实际运用： 相同的用户访问

    //    rddKV1.aggregateByKey("")
    val combineRDD = rddKV.combineByKey(
      // 初始值
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
    val combineRes = combineRDD.map {
      case (key, (sum, count)) => {
        (key, sum / count)
      }
    }
    //    combineRes.collect().foreach(println)

    // 自定义分区器 false
    val partitionRDD = rddKV.partitionBy(new HashPartitioner(2))


    // 打印出每个分区的内容
    val prtPartition1 = rddKV.mapPartitionsWithIndex {
      (t, v) => {
        v.map((x => (t, x)))
      }
    }
    prtPartition1.collect().foreach(println)


    // 打印出每个分区的内容
    val prtPartition2 = partitionRDD.mapPartitionsWithIndex {
      (t, v) => {
        v.map((x => (t, x)))
      }
    }
    prtPartition2.collect().foreach(println)



  }

}
