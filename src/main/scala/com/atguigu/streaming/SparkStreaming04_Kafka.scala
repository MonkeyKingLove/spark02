package com.atguigu.streaming

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.streaming
  * @Author: liangyu
  * @CreateTime: 2019-12-09 22:11
  * @Description: ${Description}
  */
object SparkStreaming04_Kafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "ads_log"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val dStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic))
    dStream.print()

/*    val mapStream: DStream[String] = dStream.map(_._2)
    val wordStream: DStream[String] = mapStream.flatMap(
      line => line.split(" ")
    )
    val value: DStream[(String, Int)] = wordStream.map(
      word => (word, 1)
    ).reduceByKey(_ + _)
    value.print()*/
    ssc.start()
    ssc.awaitTermination()
  }

}
