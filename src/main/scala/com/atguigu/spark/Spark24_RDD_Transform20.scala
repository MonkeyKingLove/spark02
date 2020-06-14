package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-03 18:59
  * @Description: ${Description}
  */
object Spark24_RDD_Transform20 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark24_RDD_Transform20")
    val sc: SparkContext = new SparkContext(conf)

    // 算子 - 转换 - mapValues

    //mapValues可以将k-v类型的数据，只处理value就可以，输出时会默认处理好key
    //RDD调用算子时，每个分区内的元素是串行执行的，一个接着一个的。所以算子每次接收的参数就是RDD内的对应的类型的一个元素
    val datas = List( (1,1),(1,2),(1,4),(1,6),(1,5),(1,3) )
    val rdd = sc.makeRDD(datas)
    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupByKey()
    val res = groupRDD.mapValues(
      datas => datas.toList.sortWith(
        (num1, num2) => (num1 > num2)
      ).take(3)
    )
//    println((res.collect).mkString("--"))4
    res.collect.foreach(println)
  }


}
