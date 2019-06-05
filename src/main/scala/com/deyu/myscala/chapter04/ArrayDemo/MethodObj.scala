package com.deyu.myscala.chapter04.ArrayDemo

object MethodObj {

  def main(args: Array[String]): Unit = {
    val listStr = List(3, 4, 5, 6)
    println(listStr.size)
    println(listStr.take(1))  // 3
    println(listStr.max)
    println(listStr.min)
    println(listStr.sum)   // 18

    println(listStr.head)  // 只取第一位
    println("last :" + listStr.last) // 最后一位 , 可以递归
    println("tail :" + listStr.tail)  // 除了第一位
    println("init : " + listStr.init) // 除了最后一位
//    println(list.tails.foreach(println))
    println(listStr.product) // 乘积
    // true 返回 , false 不返回
    println("filter: " + listStr.filter(x => x % 2 == 0))
    // 按照奇数偶数来分组 key - value 的方式
    println("groupBy: " + listStr.groupBy(x =>x % 2 == 0))

    // 通过首字符的相同的情况
    val arrayStr = List("spark", "hbase", "hive", "scala", "hbase")
//    val stringToStrings:Map[String, List[String]] = arrayStr.groupBy(word=>word.substring(0,1))
    // key : 首字符， value : 内容
    val stringToStrings = arrayStr.groupBy(word=>word.substring(0,1))
    println(stringToStrings)

    // key : word , value : 个数
    // word => 这边 分组的规则

    val intToStrings = arrayStr.groupBy(word=>word)
    println(intToStrings)

    // 统计个数
    // map  word => key , value
    val stringToInt = intToStrings.map(word=>(word._1, word._2.size))
    println(stringToInt)
    // 按照什么排序
    val sortArrayStr = stringToInt.toList.sortWith {
      (left, right) => {
        // 小于降序， 大于升序
        left._2 < right._2
      }
    }

    println(sortArrayStr)

    // 排序规则
//    val listSort = list.sortBy(x=>x%2)
    // 按照原来的顺序排序
    val listSort = listStr.sortBy(x=>x)

    println(listSort)


    // 扁平化处理

    val listStrStr = List(List(1,2), List(3,4), List(5,6))
    // 需要传入一个可迭代的, 扁平化处理
    val listFlatMap = listStrStr.flatMap(x=>x)
    // 处理空格后
//    val listFlatMap = listStrStr.flatMap(x=>x.split(" "))

    println(listFlatMap)




  }

}
