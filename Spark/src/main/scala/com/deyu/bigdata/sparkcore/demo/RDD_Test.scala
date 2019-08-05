package com.deyu.bigdata.sparkcore.demo

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    // 创建一个spark app 的入口
    val sc = new SparkContext(conf)
    //
    val testFile = sc.textFile("input")
    val parall = sc.parallelize(testFile.collect(),3)

    parall.saveAsTextFile("output")


  }

}
