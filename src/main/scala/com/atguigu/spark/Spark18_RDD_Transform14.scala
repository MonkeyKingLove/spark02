package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-02 23:46
  * @Description: ${Description}
  */
object Spark18_RDD_Transform14 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark18_RDD_Transform14")
    val sc: SparkContext = new SparkContext(conf)

    // 算子 - 转换 - partitionBy
    //partitionBy 重分区，参数是partitioner（分区器），repartition就是默认的分区器：hashPartitioner
    val datas = List( ("a", 1), ("b", 1), ("c", 1) )
    val rdd = sc.makeRDD(datas,1)
    val partitionRDD = rdd.partitionBy(new org.apache.spark.HashPartitioner(3))
    val result = partitionRDD.mapPartitionsWithIndex {
       (index, datas) => {


          datas
      }
    }
    result.collect()

  }

}
