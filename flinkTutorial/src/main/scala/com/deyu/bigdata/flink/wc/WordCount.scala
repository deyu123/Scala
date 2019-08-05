package com.deyu.bigdata.flink.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._


object WordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 2. source , 读取数据
    val param: ParameterTool = ParameterTool.fromArgs(args)
    val dataPath: String = param.get("input")
    val inputDS  = env.readTextFile(dataPath)
    // 3. transformation , 转换操作
    // 底层的操作可能使用到隐式转换
    val wordCount: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    // 4. sink 输出
    wordCount.print()

  }


}
