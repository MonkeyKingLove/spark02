package com.atguigu.streaming

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.streaming
  * @Author: liangyu
  * @CreateTime: 2019-12-10 20:15
  * @Description: ${Description}
  *
  */
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming08_Kafka_LowAPI {
  def main(args: Array[String]): Unit = {

    // Spark配置对象
    val conf = new SparkConf().setAppName("SparkStreaming08_Kafka_LowAPI").setMaster("local[*]")

    // SparkStreaming上下文环境对象
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.sparkContext.setLogLevel("error")

    // 使用低级API维护Offset

    // 创建Kafka集群的连接
    val brokers = "linux1:9092,linux2:9092,linux3:9092"
    val topic = "bigdata190715"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      "zookeeper.connect" -> "linux1:2181,linux2:2181,linux3:2181",
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val kafkaCluster = new KafkaCluster(kafkaParams)

    // TODO 获取偏移量
    var topicAndPartitionToOffsetMap: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()

    // 1. 获取指定Topic的分区
    val topicAndPartitionEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))
    // 判断指定的Topic是否存在分区数据
    if ( topicAndPartitionEither.isRight ) {
      // 获取指定Topic的分区
      val topicAndPartitions: Set[TopicAndPartition] = topicAndPartitionEither.right.get
      // 获取指定分区的偏移量
      val offsetEither: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(group, topicAndPartitions)
      // 判断指定分区是否已经消费过
      if (offsetEither.isLeft) {
        // 未消费过，所以偏移量从0开始
        topicAndPartitions.foreach {
          topicAndPartition => {
            topicAndPartitionToOffsetMap = topicAndPartitionToOffsetMap + (topicAndPartition -> 0)
          }
        }
      } else {
        // 已经消费过，直接消费
        val current: Map[TopicAndPartition, Long] = offsetEither.right.get
        topicAndPartitionToOffsetMap ++= current
      }
    }

    // 从kafka中获取指定偏移量的数据
    val messageDS: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      kafkaParams,
      topicAndPartitionToOffsetMap,
      (message: MessageAndMetadata[String, String]) => message.message()
    )

    // 消费数据
    messageDS.print()

    messageDS.foreachRDD(
      rdd => {
        var map: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()

        val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
        hasOffsetRanges.offsetRanges.foreach(range => {
          // 每个分区的最新的 offset
          map += range.topicAndPartition() -> range.untilOffset
        })

        // TODO 更新偏移量
        kafkaCluster.setConsumerOffsets(group,map)
      }
    )

    // 启动采集器
    ssc.start()

    ssc.awaitTermination()

  }

}
