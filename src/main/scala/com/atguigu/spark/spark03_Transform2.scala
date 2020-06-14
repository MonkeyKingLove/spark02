package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-01 22:31
  * @Description: ${Description}
  */
object spark03_Transform2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark03_Transform2")
    val context: SparkContext = new SparkContext(conf)
    val list= List(1,List(2,3),List(4,5))
    val dataRDD: RDD[Any] = context.makeRDD(list)
    val  flatMapRDD: RDD[Int] = dataRDD.flatMap(
      data => {
        data match {
          case i: Int => List(i)
          case list1: List[Int] => list1
          case _ => Nil
        }
      }
    )
    flatMapRDD.collect().foreach(println)
    context.stop()
  }

}
