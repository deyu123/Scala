package com.deyu.bigdata.sparkcore.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Value {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local")
    // 创建一个spark app 的入口
    val sc = new SparkContext(conf)

    // 构建RDD
    val numRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //map
    val numRDDMap = numRDD.map(_ * 2)
    //    numRDDMap.collect().foreach(println)

    // 传入一个 集合，返回一个集合
    //    val mp: RDD[Int] = numRDD.mapPartitions(datas=>datas)
    val mp: RDD[Int] = numRDD.mapPartitions {
      datas => {
        datas.map(_
          * 2)
      }
    }
    //    mp.collect().foreach(println)

    // RDD 转换算子  将分区转化为数组
    val pA = sc.parallelize(1 to 16, 4)
    val pArray: RDD[Array[Int]] = pA.glom()
    val mapRDD = pArray.map(_.max)

    // 每个分区的最大值
    //    mapRDD.collect().foreach(println)
    // 求集合中的最大值
    //    println(mapRDD.collect().max)
    // glom 是将里面值 RDD[Int] 转化成RDD[Array[Int]]
    val mpArr: RDD[Array[Int]] = mp.glom()
    // 从分区中求最大值
    //    println(mpArr.map(_.max).collect().max)

    val mpwi = pA.mapPartitionsWithIndex {
      // 加了一个分区号
      (x, dates) => {
//        println(x)
        // 可以得到分区号，
        dates.map(_*x)
      }
    }
//    mpwi.collect().foreach(println)

    // 只能扁平化 List(List(1,2,3,4), List(5,6), 7, 8)
    val mr = sc.makeRDD(List(List(1,2,3,4), List(5,6)))
    val fm = mr.flatMap(x=>x)
//    fm.collect().foreach(println)

    // group by
    val fg = fm.groupBy(x => x%2 ==0)
//    fg.collect().foreach(println)
    // filter by  , true 则返回
    val fmf = fm.filter(x=> x%2==0)
//    fmf.collect().foreach(println)

    // distinct

    val dis = sc.makeRDD(List(1,3,3,2,3, 5, 4,6, 8))
//    val disArr = dis.distinct()
    // 设置并行度
    val disArr = dis.distinct(3)
    // 查看分区数
//    println("disArr partitions : " + disArr.partitions.size)
//    disArr.collect().foreach(println)
    // 必须返回一个新的集合
    val newDisArr = disArr.coalesce(1)
    // 查看分区数
//    println("newDisArr partitions: " + newDisArr.partitions.size)

    // 根据分区数，随机洗牌所有数据
    val reArr = disArr.repartition(2)


//    println("reArr partitions: " + reArr.partitions.size)
    // 排序，默认是升序（正序）
//    val reSort = reArr.sortBy(x=>x)
    // false, 为降序
    val reSort = reArr.sortBy(x=>x,false)

    reSort.collect().foreach(println)

  }

}
