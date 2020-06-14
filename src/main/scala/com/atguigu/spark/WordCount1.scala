package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-05 08:35
  * @Description: ${Description}
  */
object WordCount1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark9_RDD_Transform6")
    val sc = new SparkContext(conf)
    val strList = List("Hello Scala", "Hello Spark", "Hello World")
    val strRDD: RDD[String] = sc.makeRDD(strList)
    val wordRDD: RDD[String] = strRDD.flatMap(str=>str.split(" "))
    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map(word=>(word, 1))
    //方法一：map + groupBy + map
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = wordToOneRDD.groupBy(t=>t._1)
    val wordToSumRDD: RDD[(String, Int)] = groupRDD.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    //方法二：reduceByKey
    // WordCount - 2
    val reduceRDD: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_+_)

    //方法三： groupByKey
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = wordToOneRDD.groupByKey()

    val reduceRDD1: RDD[(String, Int)] = groupByKeyRDD.map {
      case (word, list) => {
        (word, list.sum)
      }
    }
    //方法四：aggregateByKey
    val aggregateRDD: RDD[(String, Int)] = wordToOneRDD.aggregateByKey(0)(_+_,_+_)

    //方法五:foldByKey
    val foldByKeyRDD: RDD[(String, Int)] = wordToOneRDD.foldByKey(0)(_+_)

    //方法六：combineByKey
    val combineByKeyRDD: RDD[(String, Int)] = wordToOneRDD.combineByKey(
      num => num,
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x + y
    )
    //方法七：countByKey
    val countByKeyRDD: collection.Map[String, Long] = wordToOneRDD.countByKey()

    //方法八：countByValue
    val countByValueRDD: collection.Map[String, Long] = wordRDD.countByValue()
    //方法九：自定义累加器
    val acc = new MyAcc
    sc.register(acc, "MyAcc")
    wordRDD.foreach(
      word =>acc.add(word)
    )
    println(acc.value)

    //方法十：mapValue,针对的是 List((hello,2), (spark,3), (hello,3))类型
    val datas = List(("hello",2), ("spark",3), ("hello",3))
    val rdd = sc.makeRDD(datas)
    val groupRDD2: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(t=>t._1)
    val value: RDD[(String, Iterable[Int])] = groupRDD2.mapValues(
      datas => {
        datas.map(
          data => data._2
        )
      }
    )



    wordToSumRDD.collect().foreach(println)
  }

}
class MyAcc1 extends AccumulatorV2[String,ListBuffer[(String,Int)]] {
  var buffer: ListBuffer[String] = ListBuffer[String]()


  override def isZero: Boolean = buffer.isEmpty

  override def copy(): AccumulatorV2[String, ListBuffer[(String, Int)]] = ???

  override def reset(): Unit = ???

  override def add(v: String): Unit = ???

  override def merge(other: AccumulatorV2[String, ListBuffer[(String, Int)]]): Unit = ???

  override def value: ListBuffer[(String, Int)] = ???
}

class MyAcc extends AccumulatorV2[String,mutable.Map[String,Int]]{
  private var map = mutable.Map[String,Int]()
  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] ={
    new MyAcc
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v, 0) + 1
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    val map1 = map // (a, 1)
    val map2 = other.value // (a,2) => (a,3)

    map = map1.foldLeft(map2)(
      ( innerMap, kv ) => {
        innerMap(kv._1) = innerMap.getOrElse(kv._1, 0) + kv._2
        innerMap
      }
    )
  }

  override def value: mutable.Map[String, Int] = map
}
