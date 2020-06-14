package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-11-29 15:49
  * @Description: ${Description}
  */
object WordCountBySpark {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountBySpark")
    val sc: SparkContext = new SparkContext(conf)
    val arrarRDD: Array[(String, Int)] = sc.textFile("input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).collect()
    arrarRDD.foreach(println)
    sc.stop()
  }

}
