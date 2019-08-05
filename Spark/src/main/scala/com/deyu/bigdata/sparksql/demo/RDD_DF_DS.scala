package com.deyu.bigdata.sparksql.demo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object RDD_DF_DS {
  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置App名称
//    val spark = SparkSession
//      .builder().master("local")
//      .appName("Spark SQL basic example")
//      .config("spark.some.config.option", "some-value")
//      .getOrCreate()

    val conf = new  SparkConf().setMaster("local").setAppName("Spark SQL")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    // 默认都会加上
    import spark.implicits._

    val df = spark.read.json("data/people.json")

    // Displays the content of the DataFrame to stdout
//    df.show()

//    df.filter($"age" > 21).show()

    df.createOrReplaceTempView("persons")

//    spark.sql("SELECT * FROM persons where age > 21").show()

    // 1 ===========RDD => DataFrame
    // 创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan", 24), (2, "lisi", 25), (3, "wangwu", 30)))
    // 将RDD 转成 DataFrame,  需要字段
    val rddDframe = rdd.toDF("id", "name", "age")
//    rddframe.show()

    // 将 fileRDD 转成 DF
    val fileDframe = df.toDF()
//    fileframe.show()

    // 2 ===========DataFrame 转成 DataSet
    // 需要一个样例类
    val DSUser: Dataset[User] = rddDframe.as[User]
    val DSPerson = fileDframe.as[Person]
//    DSUser.show()
//    DSPerson.show()

    //3 ===========RDD 转 DataSet  map 就有字段和类型
    val userRDD = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val DSMap = userRDD.toDS()
    DSMap.show()

    //4 ===========DataSet 转 DataFrame
    // DS -> RDD 有类型， 有字段， DF -> RDD , 有字段， 无类型
    DSMap.rdd
    df.rdd
    spark.stop()

  }

}
case class User(id: Int, name:String, age:Int)
case class Person(age:String, name: String)