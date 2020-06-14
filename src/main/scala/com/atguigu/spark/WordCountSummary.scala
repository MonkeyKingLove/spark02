package com.atguigu.spark


import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-04 22:03
  * @Description: ${Description}
  */
object WordCountSummary {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCountSummary")
    val sc = new SparkContext(conf)
    val acc = new MyAcc
    sc.register(acc, "MyAcc")
    val strList = List("Hello Scala", "Hello Spark", "Hello World")
    val strRDD: RDD[String] = sc.makeRDD(strList,2)
    val wordsRDD = strRDD.flatMap(
      line => line.split(" ")
    )
    val parRDD = wordsRDD.repartition(2)
    parRDD.foreach(
      word =>acc.add(word)
    )
    println(acc.value)
    
    //一共10种：
    //1.双map,groupBy
    //先flatMap,然后groupBy,然后



    //2.reduceBykey


    //3.groupByKey


    //4.countByKey


    //5.countByValue


    //6.combineByKey


    //7.aggregateByKey


    //8.foldByKey


    //9.partitionByKey



    //10.


/*    // WordCount - 1

    val strList = List("Hello Scala", "Hello Spark", "Hello World")
    val strRDD: RDD[String] = sc.makeRDD(strList)

    // TODO 1. 将字符串拆分成一个一个的单词
    val wordRDD: RDD[String] = strRDD.flatMap(str=>str.split(" "))

    // TODO 2. 将单词结构进行转换 ： word => (word, 1)
    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map(word=>(word, 1))

    // TODO 3. 将转换结构后的数据按照单词进行分组
    // (Hello, 1), (Hello,1)
    // (Hello, 20)
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = wordToOneRDD.groupBy(t=>t._1)

    // TODO 4. 将分组后的数据进行结构的转换
    val wordToSumRDD: RDD[(String, Int)] = groupRDD.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    wordToSumRDD.collect().foreach(println)*/


    /*val conf = new SparkConf().setMaster("local[*]").setAppName("Spark19_RDD_Transform15")
    val sc: SparkContext = new SparkContext(conf)

    // 算子 - 转换 - reduceByKey, groupByKey
    val datas = List( ("a", 1), ("b", 2), ("c", 3),("a", 4) )

    val dataRDD = sc.makeRDD(datas,1)

    // WordCount - 2
    val reduceRDD: RDD[(String, Int)] = dataRDD.reduceByKey(_+_)

    // WordCount - 3
    val groupRDD: RDD[(String, Iterable[Int])] = dataRDD.groupByKey()

    val reduceRDD1: RDD[(String, Int)] = groupRDD.map {
      case (word, list) => {
        (word, list.sum)
      }
    }

    println( reduceRDD1.collect().mkString(",") )

    sc.stop()*/

  /*
    // 算子 - 转换 - aggregateByKey
    val datas = List( ("a", 100), ("b", 200), ("b", 300), ("c", 300),("a", 400),("a", 200) )

    val dataRDD = sc.makeRDD(datas,2)

  // 聚合数据
    // aggregateByKey有两个参数列表
    // 第一个参数列表只有一个参数：zeroValue，z， 表示初始值
    // 第二个参数列表有两个参数
    //      第一个参数：分区内计算规则
    //      第二个参数：分区间计算规则

    // TODO 每个分区相同key对应值的最大值，然后相加
    // reduceByKey需要分区内和分区间计算规则相同。
            val resultRDD: RDD[(String, Int)] = dataRDD.aggregateByKey(10)(
    //            (x, y) => math.max(x, y),
    //            (x, y) => x + y
    //        )
    // ("a", 1), ("b", 2), ("b", 3) => (a,10),(b,10)
    // ("c", 3),("a", 4),("a", 2)   => (c,10),(a,10)
    // => (b,10),(c,10),(a,20)
    //println(resultRDD.collect().mkString(","))

    // WordCount - 4
    //val wordToCountRDD: RDD[(String, Int)] = dataRDD.aggregateByKey(0)(_+_,_+_)
    //println(wordToCountRDD.collect().mkString(","))

    // WordCount - 5
    // 当分区内计算规则和分区间相同时，可以简化为foldByKey
    val wordToCountRDD: RDD[(String, Int)] = dataRDD.foldByKey(0)(_+_)
    println(wordToCountRDD.collect().mkString(","))
    sc.stop()*/


    /*// ("a", 100), ("b", 200), ("b", 300)
    // ("c", 300),("a", 400),("a", 200)
    //        val combineByKeyRDD: RDD[(String, (Int, Int))] = dataRDD.combineByKey(
    //            num => (num, 1),
    //            (t: (Int, Int), num: Int) => (t._1 + num, t._2 + 1),
    //            (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    //        )


    val combineByKeyRDD  = dataRDD.combineByKey[(Int,Int)](
      num => (num, 1),
      (t, num) => (t._1 + num, t._2 + 1),
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    combineByKeyRDD.collect().foreach{
      case ( key, (sum, count) ) => {
        println( key + "=" + sum/count )
      }
    }


    // WordCount - 6
    /*
    val combineByKeyRDD: RDD[(String, Int)] = dataRDD.combineByKey(
        num => num,
        (x: Int, y: Int) => x + y,
        (x: Int, y: Int) => x + y
    )
    println(combineByKeyRDD.collect().mkString(","))
    */
    sc.stop()*/


  /*  val conf = new SparkConf().setMaster("local[*]").setAppName("Spark30_RDD_Action04")
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.makeRDD(List("Hello", "Hello", "scala", "scala"),2)

    // 算子 - 行动 - countByKey
    // WordCount - 7
    //val stringToLong: collection.Map[String, Long] = rdd.map((_,1)).countByKey()
    //println(stringToLong)

    // 算子 - 行动 - countByKey
    // WordCount - 8
    val stringToLong: collection.Map[String, Long] = rdd.countByValue()
    println(stringToLong)

    sc.stop()*/


  }

}
class MyAcc2 extends AccumulatorV2[String,mutable.Map[String,Int]]{
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

