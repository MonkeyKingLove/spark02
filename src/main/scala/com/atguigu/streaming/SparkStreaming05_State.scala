package com.atguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.streaming
  * @Author: liangyu
  * @CreateTime: 2019-12-10 20:18
  * @Description: ${Description}
  */
object SparkStreaming05_State {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming05_State")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    ssc.sparkContext.setCheckpointDir("cp")
    val dataDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    val wordDS: DStream[String] = dataDS.flatMap(
      line => {
        line.split(" ")
      }
    )
    val mapDS: DStream[(String, Int)] = wordDS.map(
      word => (word, 1)
    )
    val resultDs: DStream[(String, Int)] = mapDS.updateStateByKey(
      (itor: Seq[Int], buffer: Option[Int]) => {
        Option(itor.sum + buffer.getOrElse(0))
      }
    )
    resultDs.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
