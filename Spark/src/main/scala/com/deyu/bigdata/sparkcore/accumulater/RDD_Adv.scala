package com.deyu.bigdata.sparkcore.accumulater

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Adv {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Adv")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("input/worldCount")
    val blanklines = sc.accumulator(0)
    val tmp = rdd.flatMap(line => {
      if (line == "") {
        blanklines += 1
      }
      line.split(" ")
    })
    println(tmp.count())
  }

}
