package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-03 18:56
  * @Description: ${Description}
  */
object Spark22_RDD_Transform18 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark21_RDD_Transform17")
    val sc: SparkContext = new SparkContext(conf)

    // 算子 - 转换 - aggregateByKey

    // 相同key取平均值
    //ByKey的方法的参数的都是对应的元素（k-v）的value
    val datas = List(("a", 100), ("b", 200), ("b", 300), ("c", 300), ("a", 400), ("a", 200))

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(datas, 2)
    println(dataRDD.partitioner)
    /*val groupRDD = dataRDD.groupByKey()
    val aveRDD = groupRDD.map {
      case (key, list) => {
        val ave = list.sum / list.size
        (key, ave)
      }
    }
    aveRDD.collect().foreach(println)
*/

    val result = dataRDD.combineByKey(
      data => (data, 1),
      (t1: (Int, Int), data) => ((t1._1 + data), (t1._1 + 1)),
      (tu1: (Int, Int), tu2: (Int, Int)) => ((tu1._1 + tu2._1), (tu1._2 + tu2._2))
    )
    result.collect().foreach(println)
  }

}
