package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-05 14:07
  * @Description: ${Description}
  */
object WordCount9_10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount9_10")
    val sc = new SparkContext(conf)
    val datas = List("Hello Scala", "Hello Spark", "Hello World")
    val makRDD = sc.makeRDD(datas)
    val wordsRDD = makRDD.flatMap(
      line => line.split(" ")
    )
//    val mapRDD = wordsRDD.map((_,1))
    wordsRDD.aggregate(mutable.Map[String,Int]())(
      (map,word)=>{
        map(word) = map.getOrElse(word,0) + 1
        map
      },
      (map1,map2) =>{
        map1.foldLeft(map2){
          (map,kv)=>{
            map(kv._1) = map.getOrElse(kv._1,0) + kv._2
            map
          }
        }
      }
    )







    //fold
    val datasRDD = wordsRDD.map(
      word => {
        mutable.Map(word -> 1)
      }
    )

    val stringToInt: mutable.Map[String, Int] = datasRDD.fold(mutable.Map[String, Int]())(
      (map1, map2) => {
        map1.foldLeft(map2) {
          (map, kv) => {
            map(kv._1) = map.getOrElse(kv._1, 0) + kv._2
            map
          }
        }
      }
    )
    stringToInt.foreach(println)



  }

}
