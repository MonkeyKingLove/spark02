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
object Spark12_RDD_Transform8 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark9_RDD_Transform7")
    val sc = new SparkContext(conf)
    val datas: List[Int] = List(1,2,3,4,5,6)
    val dataRDD: RDD[Int] = sc.makeRDD(datas,3)
    val sampleRDD: RDD[Int] = dataRDD.sample(
      //      false,
      true,
      0.5
      //      10
    )
    sampleRDD.collect().foreach(println)
    sc.stop()
  }

}
