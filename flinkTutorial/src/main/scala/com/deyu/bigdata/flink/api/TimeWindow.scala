package com.deyu.bigdata.flink.api

import org.apache.flink.streaming.api.scala._

// 定义样例类，传感器id，时间戳，温度
//case class SensorReading(id: String, timestamp: Long, temperature: Double)

object TimeWindow {
  def main(args: Array[String]): Unit = {

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


    val warning = high.map( sensorData => (sensorData.id, sensorData.temperature) )
    val connected = warning.connect(low)

    val coMap = connected.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )

    coMap.print("coMap")

    env.execute()
  }

}
