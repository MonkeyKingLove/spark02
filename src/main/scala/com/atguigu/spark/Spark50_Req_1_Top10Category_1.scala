package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-06 19:17
  * @Description: ${Description}
  */
object Spark50_Req_1_Top10Category_1 {
  def main(args: Array[String]): Unit = {
    //TODO:对于案例1的优化：将groupBy和聚合的过程用reduceByKey完成
    //TODO:1.创建配置文件对象和创建上下文对象
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("Spark50_Req_1_Top10Category_1")
    val sc: SparkContext = new SparkContext(conf)
    //TODO:2.获取源文件
    val datas: RDD[String] = sc.textFile("E:\\demo\\demo\\user_visit_action.txt",3)
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
    //TODO:4.清洗数据：将数据转换成需要的部分：品类id：点击、下单、支付id
   /* val rdd1: RDD[CategoryCountInfo] = userVisitActionRDD.flatMap(
      action0 => {
        if (action0.click_category_id != -1) {
          List(CategoryCountInfo(
            action0.click_category_id.toString, 1L, 0L, 0L
          ))
        } else if (action0.order_category_ids != "null") {
          val ids: Array[String] = action0.order_category_ids.split(",")
          var buffer: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()
          for (i <- ids) {
            buffer.append(CategoryCountInfo(i, 0L, 1L, 0L))
          }
          buffer
        } else if (action0.pay_category_ids != "null") {
          val ids: Array[String] = action0.order_category_ids.split(",")
          var buffer1: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()
          for (i <- ids) {
            buffer1.append(CategoryCountInfo(i, 0L, 0L, 1L))
          }
          buffer1
        } else {
          Nil
        }
      }
    )
    val result: Array[CategoryCountInfo] = rdd1.map(
      action1 => {
        (action1.categoryId, action1)
      }
    ).reduceByKey {
      (action2, action3) => {
        action2.clickCount = action2.clickCount + action3.clickCount
        action2.orderCount = action2.orderCount + action3.orderCount
        action2.payCount = action2.payCount + action3.payCount
        action2
      }
    }.map(_._2).sortBy(
      action4 => (
        action4.clickCount,
        action4.orderCount,
        action4.payCount), false).take(10)
    result.foreach(println)*/
    userVisitActionRDD.foreach(println)
    println("*********************")
    println("*********************")
    println("*********************")
    println("*********************")
    println("*********************")


    sc.stop()
  }

}
