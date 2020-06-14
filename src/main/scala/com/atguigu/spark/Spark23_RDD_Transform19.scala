package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-03 18:58
  * @Description: ${Description}
  */
object Spark23_RDD_Transform19 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark23_RDD_Transform19 ")
    val sc: SparkContext = new SparkContext(conf)
    // sortByKey练习 根据k-v的key进行排序,默认是升序
    //可以传入参数ascending:Boolean,true代表升序是默认的，false是降序
    val datas = List((11, "c"),  (1, "a"), (10, "b") )
    val rdd = sc.makeRDD(datas)
//    val res = rdd.sortByKey(true)
    val res = rdd.sortByKey(false)
    res.collect.foreach(println)
  }

}
