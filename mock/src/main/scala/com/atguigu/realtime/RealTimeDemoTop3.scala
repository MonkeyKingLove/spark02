package com.atguigu.realtime

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.realtime
  * @Author: liangyu
  * @CreateTime: 2019-12-10 15:21
  * @Description: ${Description}
  */
object RealTimeDemoTop3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RealTimeDemoTop3").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    ssc.sparkContext.setCheckpointDir("cp1")
    ssc.sparkContext.setLogLevel("error")
    //消费kafka中的数据
    //声明kafka参数
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
    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic)
    )
    val dataDS: DStream[String] = kafkaDS.map(_._2)
    val mapDS: DStream[(String, Int)] = dataDS.map(
      line => {
        val strings: Array[String] = line.split(",")
        val ts: Long = strings(0).toLong
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val day: String = sdf.format(new Date(ts))
        (day + "-" + strings(1) + "-" + strings(5), 1)
      }
    )
    val reduceDS: DStream[(String, Int)] = mapDS.updateStateByKey(
      (seq:Seq[Int],buffer:Option[Int])=>{
        Option(seq.sum + buffer.getOrElse(0))
      }
    )
    val value: DStream[((String, String), (String, Int))] = reduceDS.map {
      case (line, sum) => {
        val words: Array[String] = line.split("-")
        ((words(0), words(1)), (words(2), sum))
      }
    }
    val groupDS: DStream[((String, String), Iterable[(String, Int)])] = value.groupByKey()
    val resultDS: DStream[((String, String), List[(String, Int)])] = groupDS.mapValues(
      itor => {
        itor.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(3)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }


}
