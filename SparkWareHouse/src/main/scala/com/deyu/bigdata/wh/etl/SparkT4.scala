package com.deyu.bigdata.wh.etl

import com.deyu.bigdata.wh.bean.QueryResult
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkT4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkETL")
    val sc = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._ //隐式转换
//    需求4：使用Spark DataFrame Api统计通过各注册跳转地址(appregurl)进行注册的用户数,有时间的再写Spark Sql
    val result: Dataset[QueryResult] = sparkSession.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname," +
      "siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,website from idl.idl_member ").as[QueryResult]
    result.cache()

    result.mapPartitions(partition => {
      partition.map(item => (item.appregurl + "_" + item.website, 1))
    })
      .groupByKey(_._1)
      .mapValues(item => item._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val appregurl = keys(0)
        val website = keys(1)
        (appregurl, item._2, website)
      })

//    需求5：使用Spark DataFrame Api统计各所属网站（sitename）的用户数,有时间的再写Spark Sql

    result.mapPartitions(partiton => {
      partiton.map(item => (item.sitename + "_" + item.website, 1))
    }).groupByKey(_._1).mapValues((item => item._2)).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val sitename = keys(0)
        val website = keys(1)
        (sitename, item._2, website)
      })


//    需求6：使用Spark DataFrame Api统计各所属平台的（regsourcename）用户数,有时间的再写Spark Sql
    result.mapPartitions(partition => {
      partition.map(item => (item.regsourcename + "_" + item.website, 1))
    }).groupByKey(_._1).mapValues(item => item._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val regsourcename = keys(0)
        val website = keys(1)
        (regsourcename, item._2, website)
      })

    //需求7：使用Spark DataFrame Api统计通过各广告跳转（adname）的用户数,有时间的再写Spark Sql
    result.mapPartitions(partition => {
      partition.map(item => (item.adname + "_" + item.website, 1))
    }).groupByKey(_._1).mapValues(item => item._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val regsourcename = keys(0)
        val website = keys(1)
        (regsourcename, item._2, website)
      })

    // 需求8：使用Spark DataFrame Api统计各用户级别（memberlevel）的用户数,有时间的再写Spark Sql
    result.mapPartitions(partition => {
      partition.map(item => (item.memberlevel + "_" + item.website, 1))
    }).groupByKey(_._1).mapValues(item => item._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val regsourcename = keys(0)
        val website = keys(1)
        (regsourcename, item._2, website)
      }).show()

    // 需求9：使用Spark DataFrame Api统计各分区网站、用户级别下(website、memberlevel)的top3用户,有时间的再写Spark Sql

//    sparkSession.sql("select a.* from (select website,row_number() over( partition by website,memberlevel order by paymoney desc ) rownum from idl.idl_member ) a  where rownum <4;")
//    import org.apache.spark.sql.functions._
//    result.withColumn(
//      "rownum",
//      row_number().over(Window.partitionBy("website", "memberlevel").orderBy(desc("paymoney"))))
//      .where("rownum<4").orderBy("memberlevel", "rownum").show()
  }

}
