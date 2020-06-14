package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-05 15:20
  * @Description: ${Description}
  */
object Spark49_Req_1_Top10Category {
  def main(args: Array[String]): Unit = {
    //案例1：品类是指的产品的的分类, 一些电商品类分多级, 咱们的项目中品类类只有一级.
    // 不同的公司可能对热门的定义不一样. 我们按照每个品类的 点击、下单、支付 的量来统计热门品类
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
    sortRDD.take(10).foreach(println)

}

}

case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long)
case class CategoryCountInfo(categoryId: String,
                             var clickCount: Long,
                             var orderCount: Long,
                             var payCount: Long)