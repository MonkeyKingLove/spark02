package com.atguigu.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver


/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.streaming
  * @Author: liangyu
  * @CreateTime: 2019-12-09 22:11
  * @Description: ${Description}
  */
object SparkStreaming03_DIY {
  def main(args: Array[String]): Unit = {
    // Spark配置对象
    val conf = new SparkConf().setAppName("SparkStreaming03_DIY").setMaster("local[*]")

    // SparkStreaming上下文环境对象
    val ssc = new StreamingContext(conf, Seconds(3))

    // 操作数据源
    val ds: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("linux1", 9999))

    // 将获取的数据进行扁平化操作
    val wordDS: DStream[String] = ds.flatMap(_.split(" "))

    val wordToOneDS: DStream[(String, Int)] = wordDS.map((_,1))

    val wordToSumDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_+_)

    wordToSumDS.print()

    // 启动采集器
    ssc.start()

    ssc.awaitTermination()

  }

}

// 自定义数据采集器
// 1. 继承Receiver
// 2. 重写方法
class MyReceiver( host:String, port:Int ) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  private var socket: Socket = _
  def receive(): Unit = {
    try {
      socket = new Socket(host, port)
      val reader:BufferedReader  = new BufferedReader(
        new InputStreamReader(
          socket.getInputStream,
          "UTF-8"
        )
      )

      var s = ""
      while ( (s = reader.readLine())!= null ) {
        // ==END==
        store(s)
      }

    } catch {
      case e: ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    }


  }

  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  override def onStop(): Unit = {
    if ( socket != null ) {
      socket.close()
      socket = null
    }
  }
}
