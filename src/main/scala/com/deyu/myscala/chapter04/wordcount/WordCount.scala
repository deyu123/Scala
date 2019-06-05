package com.deyu.myscala.chapter04.wordcount

object WordCount {
  def main(args: Array[String]): Unit = {
    val stringList = List("hello spark fink hive", "hello spark fink","hello spark","hello")
    // 将list 扁平化
    val wordList = stringList.flatMap(word=>word.split(" "))
    // 按照 单词来进行分组
    val wordToWords = wordList.groupBy(word=>word)
    // 使用map 转换格式  a = (a, b)
    val wordToCount = wordToWords.map(word=>(word._1,word._2.size))
    // 按照 value 来排序,降序排序
    val sortList = wordToCount.toList.sortWith {
      (left, right) => {
        left._2 > right._2
      }
    }
    // 取前三个
    println(sortList.take(3))
    // t2
    val tuplesT2 = stringList.flatMap(x => x.split(" ")).groupBy(x => x).map(x => (x._1, x._2.size)).toList.sortWith {
      (left, right) => {
        left._2 > right._2
      }
    }.take(3)
    println(tuplesT2)
    // t3
//    val T3 = stringList.toList.flatMap(x=>{x.split(" ")}).map(x=>{(x,1)}).groupBy(x=>{x._1}).mapValues(x=>{x.size})
    val T3 = stringList.toList.flatMap(x=>x.split(" ")).map(x=>(x,1)).groupBy(x=>x._1).mapValues(x=>x.size);
    println(T3)

    // 只取前三个, 分组,map 之后得到对偶,sortWith top3
    val stringToList = stringList.flatMap(x=>x.split(" ")).groupBy(x=>x).map(x=>(x._1,x._2.size)).toList.sortWith{
          //升序
      (left, right) =>{
        left._2 > right._2
      }
    }.take(3)
    println("top 3:" + stringToList)


//    使用Scala语言实现单词数量统计功能
//    List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))
//    将上面集合中的单词统计出现次数并按照次数降序排列取前三
    val test = List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))
    val testStr = test.flatMap(x=>(x._1))




    
  }
}
