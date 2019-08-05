package com.deyu.bigdata.sparkcore.demo

import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_HBASE_GET {
  def main(args: Array[String]): Unit = {
    // 数据的写入
    val sparkConf = new SparkConf().setAppName("hbase").setMaster("local")
    val sc = new SparkContext(sparkConf)
    // 访问Hbase
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "test")
    conf.set("hbase.zookeeper.quorum", "192.168.111.202")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val count: Long = hbaseRDD.count()
    println(count)

    hbaseRDD.foreach{
      case (rk, result) => {

        val cells: Array[Cell] = result.rawCells()
        for ( cell <- cells ) {
          println(Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }
    // 关闭资源
    sc.stop()
  }

}
