package com.deyu.bigdata.sparkcore.demo

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object RDD_HBASE_PUT {
  def main(args: Array[String]): Unit = {
    // 数据的写入
    val sparkConf = new SparkConf().setAppName("hbase").setMaster("local")
    val sc = new SparkContext(sparkConf)
    // 访问Hbase
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "test")
    conf.set("hbase.zookeeper.quorum", "192.168.111.202")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val initialRDD = sc.parallelize(List(("1", "apple"), ("2", "banana"),("3", "pear")))

    // 保存在RDD 中
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "test")

    //Put
    val hbaseRDD = initialRDD.map {
      case (rk, name) => {
        val put = new Put(Bytes.toBytes(rk))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
        (new ImmutableBytesWritable(Bytes.toBytes(rk)), put)
      }
    }
    hbaseRDD.saveAsHadoopDataset(jobConf)
  }

}
