package com.deyu.bigdata.sparksql.exercise

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructType}

object UDAF_ {
  def main(args: Array[String]): Unit = {

  }

}
object UDAF_avg extends UserDefinedAggregateFunction{
  // 输入数据类型
  override def inputSchema: StructType = {
    new StructType().add("input", LongType)
  }

  // 缓存数据类型
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count", LongType)
  }

  // 输出数据类型
  override def dataType: DataType = {
    LongType
  }

  // 函数稳定性
  override def deterministic: Boolean = true

  // 初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {

  }

  // Executor 中的 合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  // 多个Executor 中的合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  // 返回结果
  override def evaluate(buffer: Row): Any = ???
}
