package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-03 10:48
  * @Description: ${Description}
  */
object Spark26_Demo {
  def main(args: Array[String]): Unit = {
    //TODO:需求: 统计出每一个省份广告被点击次数的 TOP3
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark25_Demo")
    val sc = new SparkContext(conf)
    val datas = sc.textFile("E:\\demo\\demo\\agent.log")
    //TODO:数据结构：时间戳，省份，城市，用户，广告 字段使用空格分割。
    //TODO:1.对数据进行结构转换
    val preAndAdvToOne = datas.map(
      line => {
        val words: Array[String] = line.split(" ")
        ((words(1), words(4)), 1)
      }
    )
    //TODO:2.将转换的RDD进行分组，聚合，获取相同省份，每个广告被点击的次数
  val preAndAdvToSum: RDD[((String, String), Int)] = preAndAdvToOne.reduceByKey(_ + _)
    //TODO:3.将数据进行结构转换((pre,adv),sum)=> (pre,(adv,sum))
    val preAndAdvAndSum: RDD[(String, (String, Int))] = preAndAdvToSum.map {
      case (tuple, sum) => {
        (tuple._1, (tuple._2, sum))
      }
    }
    //TODO:4.按照相同省份，进行分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = preAndAdvAndSum.groupByKey()
    //TODO:5.对每个广告的访问量按照降序进行排序
    val relust: RDD[(String, List[(String, Int)])] = groupRDD.mapValues (
      datas => {
        datas.toList.sortWith(
          (t1, t2) => {
            t1._2 > t2._2
          }
        )
      }.take(3)
    )
    relust.collect().foreach(println)
    val sum: LongAccumulator = sc.longAccumulator("sum")

    sc.stop()
    }




}
