package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-02 23:43
  * @Description: ${Description}
  */
object Spark13_RDD_Transform9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark9_RDD_Transform6")
    val sc = new SparkContext(conf)
    val strList = List("Hello Scala", "Hello Spark", "Hello World")
    val datasRDD: RDD[String] = sc.makeRDD(strList ,3)
    //distinct是去重
    val wordRDD: RDD[String] = datasRDD.flatMap(
      data => data.split(" ")
    )
    //map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
    val value = wordRDD.distinct()
    value.collect().foreach(println)


    sc.stop()
  }

}
