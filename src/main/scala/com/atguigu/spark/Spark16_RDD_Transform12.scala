package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-02 23:44
  * @Description: ${Description}
  */
object Spark16_RDD_Transform12 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark15_RDD_Transform11")
    val sc: SparkContext = new SparkContext(conf)

    // 算子 - 转换 - 双values
    val datas1 = List(1,2,1,4)
    val datas2 = List(1,5,6,7)

    val rdd1 = sc.makeRDD(datas1,2)
    val rdd2 = sc.makeRDD(datas2,2)
    //TODO:1.并集
    val unionRDD = rdd1.union(rdd2)
    unionRDD.collect.foreach(println)
    println("************************")




    //TODO：2.交集

    val intersectionRDD = rdd1.intersection(rdd2)
    intersectionRDD.collect.foreach(println)
    println("************************")

    //TODO：3.差集
    val subRDD = rdd1.subtract(rdd2)
    subRDD.collect.foreach(println)
    println("************************")

    //TODO:4.拉链
    val vipRDD = rdd1.zip(rdd2)
    vipRDD.collect.foreach(println)
    println("************************")
    //TODO：5.笛卡尔积
    val carRDD = rdd1.cartesian(rdd2)
    carRDD.collect.foreach(println)
  }

}
