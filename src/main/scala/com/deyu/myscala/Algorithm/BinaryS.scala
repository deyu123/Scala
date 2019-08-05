package com.deyu.myscala.Algorithm

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object BinaryS {

  /**
    * 二分查找(折半查找)优点是比较次数少，查找速度快，平均性能好；其缺点是要求待查表为有序表，且插入删除困难。时间复杂度可以表示O(h)=O(log2n)，
    * 以2为底，n的对数。比如数组长度为10，最多找4次。
    * {1,8, 10, 89, 1000, 1000，1234} 当一个有序数组中，有多个相同的数值时，如何将所有的数值都查找到，比如这里的 1000.
    * //分析
    *1. 返回的结果是一个可变数组 ArrayBuffer
    *2. 在找到结果时，向左边扫描，向右边扫描 [条件]
    *3. 找到结果后，就加入到ArrayBuffer
    */

  def binarySearch2(arr: Array[Int], l: Int, r: Int,
                    findVal: Int): ArrayBuffer[Int] = {

    //找不到条件?
    if (l > r) {
      return ArrayBuffer()
    }

    val midIndex = (l + r) / 2
    val midVal = arr(midIndex)
    if (midVal > findVal) {
      //向左进行递归查找
      binarySearch2(arr, l, midIndex - 1, findVal)
    } else if (midVal < findVal) { //向右进行递归查找
      binarySearch2(arr, midIndex + 1, r, findVal)
    } else {
      println("midIndex=" + midIndex)
      //定义一个可变数组
      val resArr = ArrayBuffer[Int]()
      //向左边扫描
      var temp = midIndex - 1
      breakable {
        while (true) {
          if (temp < 0 || arr(temp) != findVal) {
            break()
          }
          if (arr(temp) == findVal) {
            resArr.append(temp)
          }
          temp -= 1
        }
      }
      //将中间这个索引加入
      resArr.append(midIndex)
      //向右边扫描
      temp = midIndex + 1
      breakable {
        while (true) {
          if (temp > arr.length - 1 || arr(temp) != findVal) {
            break()
          }
          if (arr(temp) == findVal) {
            resArr.append(temp)
          }
          temp += 1
        }
      }
      return resArr
    }
  }

  def binarySearch(arr: Array[Int], start: Int, end: Int, value: Int): Int = {
    if (start > end) {
      return -1
    }
    var left = start
    var right = end
    val mid = start + (end - start) / 2
    if (arr(mid) == value)
      return mid
    if (arr(mid) < value) {
      left = mid + 1
    }
    if (arr(mid) > value) {
      right = mid - 1
    }
    binarySearch(arr, left, right, value)
  }

  def main(args: Array[String]): Unit = {
    // 二分查找必须是有序的
    val arr = Array(0,1, 2, 3, 7,12, 23, 43)
    println(binarySearch(arr, 0, arr.length-1, 43))

  }
}
