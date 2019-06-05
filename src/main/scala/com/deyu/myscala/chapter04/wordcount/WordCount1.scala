package com.deyu.myscala.chapter04.wordcount

object WordCount1 {
  def main(args: Array[String]): Unit = {

//    使用Scala语言实现单词数量统计功能
//    List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))
//    将上面集合中的单词统计出现次数并按照次数降序排列取前三
    val test = List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    // 1
    val testStr1 = test.map(t=>{(t._1 + " ")*t._2}).flatMap(x=>x.split(" ")).groupBy(x=>x).map(x=>(x._1,x._2.size)).toList.sortWith{
      (left, right) => {
        left._2 > right._2
      }
    }.take(3)
    println(testStr1)

    // 2
    val testStr2 = test.flatMap(x=>x._1.split(" ")).map(x=>(x,1)).groupBy(x=>x._1).mapValues(x=>x.size).toList.sortWith{
      (left, right) => {
        left._2 > right._2
      }
    }.take(3)
    println(testStr2)

    // 3
    val testStr3 = test.flatMap(x=>x._1.split(" ")).map(x=>(x,1)).groupBy(x=>x._1).mapValues(x=>x.size).toList.sortBy(_._2).reverse.take(3)
    println(testStr3)

    // 4
    val testStr4 = test.flatMap(_._1.split(" ")).map(_->1).groupBy(_._1).mapValues(_.size).toList.sortBy(_._2).reverse.take(3)
    println(testStr4)

    // 第二种
    // ((hello, spark, scala),4) | (hello 4, scala 3) (hello 3, scala 2) | hello 10,

//    test.map {
//      x =>
//    }

  }
}
