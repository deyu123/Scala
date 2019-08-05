package com.deyu.bigdata.sparkcore.demo

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

object RDD_MySQL_SAVE {
  def main(args: Array[String]): Unit = {
    // 查询MySql 中数据
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop202:3306/rdd"
    val username = "root"
    val passwd = "000000"
    // 向MySQL 中添加数据
    val conf = new SparkConf().setAppName("WC").setMaster("local")
    val sc = new SparkContext(conf)
    val dataRDD = sc.makeRDD(List((1, "zhangsan", 30), (1, "lisi", 20), (1, "wangwu", 25)), 2)


    val fp = dataRDD.foreachPartition(datas => {
      // 因为在分布式中不同的节点上运行的， 所以有些对象是需要序列化的
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, username, passwd)
      var sql = "insert into user (id, name, age) values (?, ?, ?) "
      val statement: PreparedStatement = connection.prepareStatement(sql)
      datas.foreach {
        case (id, name, age) => {
          statement.setInt(1, id)
          statement.setString(2, name)
          statement.setInt(3, age)
          statement.executeUpdate()
        }
      }

      statement.close()
      connection.close()
    })
    println(fp)

  }

}
