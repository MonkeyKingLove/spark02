package com.atguigu.spark

import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-06 19:18
  * @Description: ${Description}
  */
object Spark51_Req_1_Top10Category_2 {
  def main(args: Array[String]): Unit = {
    //TODO:对于案例1的优化：将groupBy和聚合的过程用reduceByKey完成,对于reduceByKey进行优化，用累加器
    //TODO:1.创建配置文件对象和创建上下文对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark50_Req_1_Top10Category_1")
    val sc: SparkContext = new SparkContext(conf)
    
    //TODO:2.获取源文件
    val datas: RDD[String] = sc.textFile("E:\\demo\\demo\\user_visit_action.txt")
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
    val rdd1: RDD[CategoryCountInfo] = userVisitActionRDD.flatMap(
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
    val acc: myAcc1 = new myAcc1
    sc.register(acc,"myAcc1")
    rdd1.foreach(
      action1 =>{
        acc.add(action1)
      }
    )
    //TODO:得到的是每个品类中每种点击的数量
    val value: mutable.Map[String, Int] = acc.value

    //TODO：需求是得到每个品类对应的：每种点击点击类型的数量
    val groupMap: Map[String, mutable.Map[String, Int]] = value.groupBy(
      action2 => {
        action2._1.split(":")(0)
      }
    )
    val map1: immutable.Iterable[CategoryCountInfo] = groupMap.map(
      action3 => {
        CategoryCountInfo(
          action3._1,
          action3._2.getOrElse(action3._1 + ":" + "click", 0).toLong,
          action3._2.getOrElse(action3._1 + ":" + "order", 0).toLong,
          action3._2.getOrElse(action3._1 + ":" + "payMent", 0).toLong
        )
      }
    )
    map1.toList.sortWith(
      (left, right) => {
        if ( left.clickCount > right.clickCount ) {
          true
        } else if (left.clickCount == right.clickCount) {
          if ( left.orderCount > right.orderCount ) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10).foreach(println)
  }

}
//累加器，一般都是
class myAcc1 extends AccumulatorV2[CategoryCountInfo,mutable.Map[String,Int]]{
  var map:mutable.Map[String, Int] = mutable.Map[String,Int]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[CategoryCountInfo, mutable.Map[String, Int]] = new myAcc1

  override def reset(): Unit = map.clear()

  override def add(v: CategoryCountInfo): Unit = {
    if(v.clickCount > 0L){
      map(v.categoryId + ":" + "click") = map.getOrElse(v.categoryId + ":" + "click",0) + 1
    }else if(v.orderCount > 0){
      map(v.categoryId + ":" + "order") = map.getOrElse(v.categoryId + ":" + "order",0) + 1
    }else{
     map(v.categoryId + ":" + "payMent") = map.getOrElse(v.categoryId + ":" + "payMent",0) + 1
    }
  }

  override def merge(other: AccumulatorV2[CategoryCountInfo, mutable.Map[String, Int]]): Unit = {
    val map1 = map
    val map2 = other.value
    map = map1.foldLeft(map2)(
      (innerMap, kv) => {
        innerMap(kv._1) = innerMap.getOrElse(kv._1, 0) + kv._2
        innerMap
      }
    )

  }

  override def value: mutable.Map[String, Int] = map
}
