package com.atguigu.realtime

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.realtime
  * @Author: liangyu
  * @CreateTime: 2019-12-10 21:05
  * @Description: ${Description}
  */
object HourAdsClickCount {
//  统计各广告最近 1 小时内的点击量趋势：各广告最近 1 小时内各分钟的点击量


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HourAdsClickCount")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(2))
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "ads_log"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG-> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val dataDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic)
    )
    val mapDS: DStream[String] = dataDS.map(_._2)
    //TODO:窗口大小3分钟，步长30秒
    val winDS: DStream[String] = mapDS.window(Minutes(2),Seconds(10))
    //TODO:1575988236372,华北,北京,104,4 数据格式：时间+地区+城市+用户+广告
//    mapDS.print()
    val map1DS: DStream[((String, String), Int)] = winDS.map(
      line => {
        val words: Array[String] = line.split(",")
        val sdf: SimpleDateFormat = new SimpleDateFormat("yy:MM:dd HH:mm:ss")
        val ts: Long = words(0).toLong
        val time: String = sdf.format(new Date(ts))
        ((words(4), time.init + "0"), 1)
      }
    )
    val timeToSumDS: DStream[((String, String), Int)] = map1DS.reduceByKey(_+_)
    val result: DStream[((String, String), Int)] = timeToSumDS.transform(
      rdd => rdd.sortByKey()
    )
    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
