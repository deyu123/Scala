package com.deyu.myscala.chapter06

object WorldCountDemo {
  def main(args: Array[String]): Unit = {
    //        使用Scala语言实现单词数量统计功能
    //        List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))
    //        将上面集合中的单词统计出现次数并按照次数降序排列取前三
    val listTuple = List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))
    val listTupleNum = listTuple.map(x => (x._1 + " ") * x._2)
    //    println(listTupleNum)
    // 有输入有返回的，尽量不要省略，无法确定类型， 不知道是函数，还是变量
    val listStr = listTupleNum.flatMap(_.split(" "))
    //println(listStr)
    val listGroup = listStr.groupBy(x => x)
    //    println(listGroup)
    // map  key , value
    val listGroupMap = listGroup.map(x => (x._1, x._2.size))
    //    println(listGroupMap.toList.sortBy(_._2).reverse.take(3))


    // 第二种  hello 4, spark 3

    val wordNumList = listTuple.map {
      x => {
        // 将 hello spark 拆分
        // x 需要返回， yield 需要返回值的时候
        for (elem <- x._1.split(" ").toList) yield {
          (elem, x._2)
        }
      }
    }.flatMap(x => x).groupBy(_._1).map {
      x => {
        var sum: Int = 0
        for (elem <- x._2) {
          sum = sum + elem._2
        }
        (x._1, sum)
      }
    }.toList.sortBy(_._2).reverse.take(3)
    //    println(wordNumList)


    val tuplesList = listTuple.flatMap {
      x => {
        val worlds = x._1.split(" ")
        // list 对偶
        worlds.map(y => (y, x._2))

      }
    }.groupBy(_._1).map {
      x => {
        // list 用map 处理，
        (x._1, x._2.map(_._2).toList.sum)
      }
    }.toList.sortBy(_._2).reverse.take(3)
    //    println(tuplesList)


    val tuplesList1 = listTuple.flatMap {
      x => {
        val worlds = x._1.split(" ")
        // list 对偶
        worlds.map(y => (y, x._2))

      }
    }.groupBy(_._1).mapValues {
      x => {
        // 只关心 value , 不关心 key
        x.map(_._2).sum
      }
    }

    println(tuplesList1)


  }

}
