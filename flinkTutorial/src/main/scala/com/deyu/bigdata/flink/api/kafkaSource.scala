package com.deyu.bigdata.flink.api

import java.util.Properties

import org.apache.flink.streaming.api.scala._

// 定义样例类，传感器id，时间戳，温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object kafkaSource {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop202:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env
      .fromCollection(List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718201, 15.402984393403084),
        SensorReading("sensor_7", 1547718202, 6.720945201171228),
        SensorReading("sensor_10", 1547718205, 38.101067604893444)
      ))

    val splitStream = stream1
      .split( sensorData => {
        if (sensorData.temperature > 30) Seq("high") else Seq("low")
      } )

    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high", "low")

//    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
//
//    stream3.print("stream3")


    stream1.print("stream1")

    env.execute()

  }

}
