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
object Spark9_RDD_Transform6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark9_RDD_Transform6")
    val sc = new SparkContext(conf)
//    val datas: List[Int] = List(1,2,3,4,5,6)
    val datas1 = List( "Hello", "Hive", "Hadoop", "Spark", "Scala" )
 /*   val dataRDD = sc.makeRDD(datas,3)
    val groupRDD: RDD[(Boolean, Iterable[Int])] = dataRDD.groupBy(
      data => data % 2 == 0
    )*/
    val dataRDD = sc.makeRDD(datas1,3)
    val groupRDD: RDD[(String, Iterable[String])] = dataRDD.groupBy(
      data => data.substring(0, 1)
    )
    //(false,CompactBuffer(1, 3, 5))
    //(true,CompactBuffer(2, 4, 6))
    //分组规则的返回值就是分组的key
    groupRDD.collect().foreach(println)
    sc.stop()

  }

}
