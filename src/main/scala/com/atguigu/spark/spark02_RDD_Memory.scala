package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-01 22:10
  * @Description: ${Description}
  */
object spark02_RDD_Memory {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark02_RDD_Memory")
    val sc: SparkContext = new SparkContext(conf)
    val list: List[Int] = List(1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7)
    list.distinct
    val dataRDD: RDD[Int] = sc.makeRDD(list,2)
    dataRDD.distinct()
    val mapRDD: RDD[Int] = dataRDD.mapPartitions(
      data => {
        data.map(_ * 2)
      }
    )
    //collect是什么意思？
    mapRDD.collect().foreach(println)
    println("***************")
    println("***************")
    println("***************")
    sc.stop()
//    mapRDD.saveAsTextFile("output")
  }

}
