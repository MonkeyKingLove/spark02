package com.atguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.streaming
  * @Author: liangyu
  * @CreateTime: 2019-12-09 22:10
  * @Description: ${Description}
  */
object SparkStreaming02_Source {
  def main(args: Array[String]): Unit = {
    // Spark配置对象
    val conf = new SparkConf().setAppName("SparkStreaming02_Source").setMaster("local[*]")

    // SparkStreaming上下文环境对象
    val ssc = new StreamingContext(conf, Seconds(3))

    // 操作数据源
    val ds: DStream[String] = ssc.textFileStream("in")

    // 将获取的数据进行扁平化操作
    val wordDS: DStream[String] = ds.flatMap(_.split(" "))

    val wordToOneDS: DStream[(String, Int)] = wordDS.map((_,1))

    val wordToSumDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_+_)

    wordToSumDS.print()

    // 启动采集器
    ssc.start()

    // 默认情况下。上下文环境对象不能关闭
    //ssc.stop()
    // 优雅的关闭
    //ssc.stop()
    // 上下文环境对象不能关闭，必须等待采集器的结束
    ssc.awaitTermination()

    //new Thread().stop()
  }

}
