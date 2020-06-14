package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-02 22:33
  * @Description: ${Description}
  */
object Spark11_RDD_Transform7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark9_RDD_Transform6")
    val sc = new SparkContext(conf)
    val datas: List[Int] = List(1,2,3,4,5,6)
    val dataRDD: RDD[Int] = sc.makeRDD(datas)
    val filterRDD: RDD[Int] = dataRDD.filter(
      data => data % 2 == 1
    )
    filterRDD.collect().foreach(println)
    sc.stop()
  }

}
