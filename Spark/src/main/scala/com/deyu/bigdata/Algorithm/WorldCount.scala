package com.deyu.bigdata.Algorithm

import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WorldCount")
    val sc = new SparkContext(conf)
    sc.textFile("in").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2, false).saveAsTextFile("output")
  }
}
