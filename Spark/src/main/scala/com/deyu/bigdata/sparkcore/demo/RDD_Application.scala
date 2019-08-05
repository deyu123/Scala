package com.deyu.bigdata.sparkcore.demo

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Application {
  def main(args: Array[String]): Unit = {

    //时间戳，省份，城市，用户，广告，中间字段使用空格分割。
    // 统计出每一个省份广告被点击次数的TOP3
    val conf = new SparkConf().setMaster("local").setAppName("WC")
    val sc = new SparkContext(conf)
    // TODO 1 : 读取文件
    val rdd = sc.textFile("input/agent.log")
    // TODO 2 : 只获取 省份，广告 =>（省份_广告,1)
    val advToClick = rdd.map {
      datas => {
        val data = datas.split(" ")
        (data(1) + "_" + data(4), 1)
      }
    }

    // TODO 3:分组求和
    val advToClickSum = advToClick.reduceByKey(_ + _)
    // TODO 4:省份 分区 (广告，点击次数）
    val proMap = advToClickSum.map {
      data => {
        val d = data._1.split("_")
        (d(0), (d(1), data._2))
      }
    }

    // TODO 5: 按照省份分组
    val groupRDD = proMap.groupByKey()
    // TODo 6: 次数排序去前三
    val result = groupRDD.mapValues {
      datas => {
        datas.toList.sortWith {
          (left, right) => {
            left._2 > right._2
          }
        }
      }.take(3)
    }
    result.collect().foreach(println)
  }

}
