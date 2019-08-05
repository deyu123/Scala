package com.deyu.bigdata.sparksql.exercise

import org.apache.spark.sql.SparkSession

object SQLExer {

  // 创建一个样例类
  // case class的定义要在引用case class函数的外面。因为我只有一个main函数，所以把case class挪到了外面，然后好了
  // Error:(25, 15) value toDS is not a member of org.apache.spark.rdd.RDD[tbStock]
  //    tbStockDS.toDS()
  case class tbStock(ordernumber: String, locationid: String, dateid: String) extends Serializable

  case class tbStockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double, amount: Double) extends Serializable

  case class tbDate(dateid: String, years: Int, theyear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int, period: Int, halfmonth: Int) extends Serializable

  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置App名称
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    // 使用隐式转化
    import spark.implicits._

    val tbStockRdd = spark.sparkContext.textFile("data/tbStock.txt")
    // 处理后转成DataSet
    val tbStockDS = tbStockRdd.map(_.split(",")).map(attr => tbStock(attr(0), attr(1), attr(2))).toDS()
//    tbStockDS.show()

    val tbStockDetailRdd = spark.sparkContext.textFile("data/tbStockDetail.txt")
    val tbStockDetailDS = tbStockDetailRdd.map(_.split(",")).map(attr => tbStockDetail(attr(0), attr(1).trim().toInt, attr(2),
      attr(3).trim().toInt, attr(4).trim().toDouble, attr(5).trim().toDouble)).toDS
//    tbStockDetailDS.show()

    val tbDateRdd = spark.sparkContext.textFile("data/tbDate.txt")
    val tbDateDS = tbDateRdd.map(_.split(",")).map(attr => tbDate(attr(0), attr(1).trim().toInt,
      attr(2).trim().toInt, attr(3).trim().toInt, attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt,
      attr(7).trim().toInt, attr(8).trim().toInt, attr(9).trim().toInt)).toDS

//    tbDateDS.show()

    tbStockDS.createOrReplaceTempView("tbStock")
    tbDateDS.createOrReplaceTempView("tbDate")
    tbStockDetailDS.createOrReplaceTempView("tbStockDetail")

//    统计所有订单中每年的销售单数、销售总额
//    三个表连接后以count(distinct a.ordernumber)计销售单数，sum(b.amount)计销售总额
//    spark.sql("select * from tbStock").show()
    spark.sql("select * from tbDate").show()
//    spark.sql("select * from tbStockDetail").show()

    spark.sql("select * from tbStock t1 JOIN  tbDate t2 ON t1.dateid = t2.dateid").show()
  }

}
