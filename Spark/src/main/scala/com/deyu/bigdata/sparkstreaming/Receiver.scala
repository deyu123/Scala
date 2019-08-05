package com.deyu.bigdata.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Receiver {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("worldCount")
    val context = new StreamingContext(conf, Seconds(3))

    val socket = context.receiverStream(new MyReceiver("hadoop202", 9999))
    val world = socket.flatMap(_.split(" ").map((_,1))).reduceByKey(_+_)

    world.print()

    context.start()
    context.awaitTermination()

  }
}

class MyReceiver(host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  var socket: Socket = _

  def receive(): Unit = {
    // socket 需要传递 地址和端口
    socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,  "UTF-8"))
    var line:String = ""
    while((line = reader.readLine()) != null){
      // 需要添加一个结束标志
      val line = reader.readLine()
      if("===END===".equals(line)){
        return
      }else{
        store(line)
      }
    }
  }

  override def onStart(): Unit = {
    new Thread({
      new Runnable {
        override def run(): Unit = {
          receive()
        }
      }
    }).start()

  }

  override def onStop(): Unit = {

    if(socket != null){
      socket.close()
      socket = null
    }

  }
}