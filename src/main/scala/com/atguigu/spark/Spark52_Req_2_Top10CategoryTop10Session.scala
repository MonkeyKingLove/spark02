package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-05 20:30
  * @Description: ${Description}
  */
object Spark52_Req_2_Top10CategoryTop10Session {
  def main(args: Array[String]): Unit = {
//    对于排名前 10 的品类，分别获取每个品类点击次数排名前 10 的 sessionId。(注意: 这里我们只关注点击次数, 不关心下单和支付次数)
//    这个就是说，对于 top10 的品类，每一个都要获取对它点击次数排名前 10 的 sessionId。
//    这个功能，可以让我们看到，对某个用户群体最感兴趣的品类，各个品类最感兴趣最典型的用户的 session 的行为。
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark49_Req_1_Top10Category")
    val sc = new SparkContext(conf)

    //TODO:1.获取数据
    val datas: RDD[String] = sc.textFile("E:\\demo\\demo\\user_visit_action.txt")
    //TODO：2.将数据转换成需要的部分：品类id：点击、下单、支付id
    val userVisitActionRDD: RDD[UserVisitAction] = datas.map(
      line => {
        val ids = line.split("_")
        UserVisitAction(
          ids(0),
          ids(1).toLong,
          ids(2),
          ids(3).toLong,
          ids(4),
          ids(5),
          ids(6).toLong,
          ids(7).toLong,
          ids(8),
          ids(9),
          ids(10),
          ids(11),
          ids(12).toLong
        )
      }
    )
    //TODO:扁平化，首先拿到对应的集合或者迭代器，然后再扁平化
    val catagoryRDD: RDD[CategoryCountInfo] = userVisitActionRDD.flatMap(
      obj => {
        if (obj.click_category_id != -1) {
          List(CategoryCountInfo(obj.click_category_id.toString, 1L, 0L, 0L))
        } else if (obj.order_category_ids != "null") {
          val buffer = new ListBuffer[CategoryCountInfo]
          val category_ids: Array[String] = obj.order_category_ids.split(",")
          for (i <- category_ids) {
            buffer.append(CategoryCountInfo(i, 0L, 1L, 0L))
          }
          buffer
        } else if (obj.pay_category_ids != "null") {
          val buffer1 = new ListBuffer[CategoryCountInfo]
          val category_ids: Array[String] = obj.pay_category_ids.split(",")
          for (i <- category_ids) {
            buffer1.append(CategoryCountInfo(i, 0L, 1L, 0L))
          }
          buffer1
        }
        else {
          Nil
        }
      }
    )
    //TODO:拿到了对应的（类型，1,0,0）类型的RDD，就是单独的每个商品品类,然后分组，合并
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = catagoryRDD.groupBy(
      catagory => {
        catagory.categoryId
      }
    )
    //TODO：分组之后：每个商品品类为key，
    val reduceRDD: RDD[CategoryCountInfo] = groupRDD.mapValues(
      list => {
        list.reduce(
          (cate1, cate2) => {
            cate1.clickCount = cate1.clickCount + cate2.clickCount
            cate1.orderCount = cate1.orderCount + cate2.orderCount
            cate1.payCount = cate1.payCount + cate2.payCount
            cate1
          }
        )
      }
    ).map(_._2)
    val sortRDD: RDD[CategoryCountInfo] = reduceRDD.sortBy(
      cate => (
        cate.clickCount,
        cate.orderCount,
        cate.payCount
      ),
      false
    )
    val cateArray: Array[CategoryCountInfo] = sortRDD.take(10)
    val ids: Array[String] = cateArray.map(
      catagory => {
        catagory.categoryId
      }
    )
//    val str: String = ids.mkString(",")
//    println(str)
//    val str: String = cateArray.mkString(",")
    //TODO:过滤数据只保留前十的品类中对应的点击数据
    val filterRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(
      info => {
        ids.contains(info.click_category_id.toString)
      }
    )
    val mapRDD: RDD[((String, String), Int)] = filterRDD.map(
      action => {
        ((action.click_category_id.toString, action.session_id), 1)
      }
    )
    val sessionRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    val map1RDD: RDD[(String, (String, Int))] = sessionRDD.map(
      datas => {
        (datas._1._1, (datas._1._2, datas._2))
      }
    )
    //    val groupRDD: RDD[(String, Iterable[(String, Int)])] = map1RDD.groupByKey()
    val groupRDD1: RDD[(String, Iterable[(String, Int)])] = map1RDD.groupByKey(2)
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD1.mapValues(
      list => {
        list.toList.sortWith(
          (list1, list2) => (list1._2 > list2._2)
        ).take(10)
      }
    )
//    resultRDD.collect().foreach(println)

    val relust = resultRDD.flatMapValues(
      list => list
    )
    relust.collect().foreach(println)






  }

}
