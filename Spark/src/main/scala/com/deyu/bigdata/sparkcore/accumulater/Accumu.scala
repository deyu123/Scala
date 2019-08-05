package com.deyu.bigdata.sparkcore.accumulater

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2


object Accumu {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WC")
    val sc = new SparkContext(conf)

    val wordRDD = sc.makeRDD(List("hello", "hadoop", "spark", "scala"))

    // 创建累加器
    val accumulater = new BlackListAccumulater()
    // 注册累加器
    sc.register(accumulater, "blackList")

    wordRDD.foreach{
      word => {
        accumulater.add(word)
      }
    }

    // 获取累加器的值
    println(accumulater.value)

  }

}

class BlackListAccumulater extends AccumulatorV2[String, java.util.HashSet[String]]{
  // 黑名单集合
  var blackSet = new util.HashSet[String]
  // 判断当前累加器是否为初始状态
  override def isZero: Boolean = {
    blackSet.isEmpty
  }

  // 复制累加器
  override def copy(): AccumulatorV2[String, util.HashSet[String]] = {
    new BlackListAccumulater()
  }

  // 重置累加器
  override def reset(): Unit = {
    blackSet.clear()
  }

  // 增加数据
  override def add(v: String): Unit = {
    if(v.contains("h")){
      blackSet.add(v)
    }
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, util.HashSet[String]]): Unit = {
    blackSet.addAll(other.value)

  }

  override def value: util.HashSet[String] = {
    blackSet
  }
}
