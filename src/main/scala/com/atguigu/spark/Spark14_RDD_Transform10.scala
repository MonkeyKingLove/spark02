package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-02 23:43
  * @Description: ${Description}
  */
object Spark14_RDD_Transform10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark14_RDD_Transform10")
    val sc: SparkContext = new SparkContext(conf)

    // 算子 - 转换 - coalesce
    val datas = List(1,2,3,4,5,6,7)
    val dataRDD = sc.makeRDD(datas,3)
    val mapRDD = dataRDD.mapPartitionsWithIndex(
      (index, datas) => {
        datas.foreach(
          data => {
            println(index + ":" + data)
          }
        )
        datas
      }
    )
    mapRDD.collect()


    val coaRDD = dataRDD.coalesce(2,true)
    val indexRDD = coaRDD.mapPartitionsWithIndex(
      (index, datas) => {
        datas.foreach(
          data => {
            println(index + ":" + data)
          }
        )
        datas
      }
    )
    indexRDD.collect()
    sc.stop()
  }

}
