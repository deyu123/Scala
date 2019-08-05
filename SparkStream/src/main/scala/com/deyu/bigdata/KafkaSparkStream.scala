package com.deyu.bigdata

import kafka.serializer.StringDecoder
import net.ipip.ipdb.City
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSparkStream {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并初始化SSC
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkStream")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    // 更新状态操作时，更新检查点的信息
    ssc.sparkContext.setCheckpointDir("cp")

    //3.将kafka参数映射为map
    val kafkaParam: Map[String, String] = Map[String, String](
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "spark",
      "zookeeper.connect" -> "hadoop202:2181"
    )
    //      4.通过KafkaUtil创建kafkaDSteam
    val kafkaDSteam: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParam,
      Map("user-behavior" -> 3),
      StorageLevel.MEMORY_ONLY
    )

    //5.对kafkaDSteam做计算（WordCount）

    //    val world = kafkaDSteam.flatMap(_._2.split(" ").map((_,1))).updateStateByKey[Int]{
    //      (seq:Seq[Int], opt:Option[Int]) => {
    //        val sum = opt.getOrElse(0) + seq.sum
    //        Option(sum)
    //      }
    //    }

    val result: DStream[((String, String), Int)] = kafkaDSteam.filter(rdd => {
      val splitRDD: Array[String] = rdd._2.split("\t")
      if (splitRDD.length == 17 && splitRDD(15) == "completeOrder") {
        true
      } else {
        false
      }
    })
      .map(reg => {
        println(reg._1)
        val splitRDD: Array[String] = reg._2.split("\t")
        new City()
        splitRDD(8)
      }
      )
      .updateStateByKey[Int] {
      (seq: Seq[Int], opt: Option[Int]) => {
        val sum = opt.getOrElse(0) + seq.sum
        Option(sum)
      }
    }


    result.print()


    //6.启动SparkStreaming
    ssc.start()
    ssc.awaitTermination()
  }

}
