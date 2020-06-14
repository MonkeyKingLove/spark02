package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-02 22:32
  * @Description: ${Description}
  */
object Spark10_RDD_WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark9_RDD_Transform6")
    val sc = new SparkContext(conf)
    val datas = List("Hello Scala", "Hello Spark", "Hello World")
//    val datas = List( ("a", 1), ("b", 2), ("c", 3),("a", 4) )
        val datasRDD: RDD[String] = sc.makeRDD(datas ,2)
        //扁平化
//        val datasRDD: RDD[(String, Int)] = sc.makeRDD(datas,2)
//        datasRDD.saveAsTextFile("output")
       val wordRDD: RDD[String] = datasRDD.flatMap(
            data => data.split(" ")
          )
       //映射成元组
       val mapRDD: RDD[(String, Int)] = wordRDD.map(
         word => (word, 1)
       )
       //分组聚合
       val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(
         data => data._1
       )
//       val groupRDD: RDD[(String, Iterable[(String, Int)])] = datasRDD.groupBy(
//         data => data._1
//       )
    //
  /*  val resultRDD: RDD[(String, Int)] = groupRDD.map(
      data => (data._1, data._2.size)
    )*/
   /* val value = groupRDD.mapPartitionsWithIndex(
      (index, datas) => {
        datas.foreach(
          data => println(index + ":" + data)
        )
        datas
      }
    )

    value.collect()*/
    /*val resultRDD: RDD[(String, Int)] = groupRDD.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    resultRDD.collect().foreach(println)*/
    //
    sc.stop()
  }

}
