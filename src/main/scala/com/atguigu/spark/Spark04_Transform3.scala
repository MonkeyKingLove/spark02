package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-01 22:44
  * @Description: ${Description}
  */
object Spark04_Transform3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("")
    val sc = new SparkContext(conf)
    val datas = List(1, 3, 2, 5, 4, 6)
    val dataRDD = sc.makeRDD(datas,3)
    val splitRDD: RDD[Array[Int]] = dataRDD.glom()
    val maxRDD: RDD[Int] = splitRDD.map(
      i => i.max
    )
    //这里不用collect也会执行前面的所有的操作，为什么没有action动作也会执行呢
    //加了collect输出结果不是14.0，而是14
    println(maxRDD.collect().sum)
  }

}
