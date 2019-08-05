package com.deyu.bigdata.flink.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1. 流式处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val param: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = param.get("host")
    val port: Int = param.getInt("port")

    println("host:" + host)
    println("port:" + port)

    // 2. source,  动态读取数据，用http 发送
    val sockDS: DataStream[String] = env.socketTextStream(host,port)

    // 3. transformation
    // keyBy 会影响顺序
    val wordCount: DataStream[(String, Int)] = sockDS.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)

    // 4. sink 输出
    // 如果需要按照顺序，可以设置并行度
    wordCount.print().setParallelism(1)

    // 5. 启动executor
    env.execute()

  }

}
