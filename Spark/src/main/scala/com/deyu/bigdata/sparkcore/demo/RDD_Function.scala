package com.deyu.bigdata.sparkcore.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDD_Function {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("function")
    val sc = new SparkContext(conf)
    // 传递一个rdd

    val search = new Search("h")
    val rdd = sc.parallelize(Array("hadoop", "spark", "hive", "deyu"))
    val match1 = search.getMatch1(rdd)
//    match1.collect().foreach(println)

    val rf = rdd.filter(x=>x.contains("h"))
    rf.collect().foreach(println)
  }

}
class Search(query:String) extends  Serializable {

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }

}
